/*
 * Copyright 2017-present Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <folly/executors/ThreadPoolExecutor.h>

#include <folly/executors/GlobalThreadPoolList.h>
#include <folly/synchronization/AsymmetricMemoryBarrier.h>

namespace folly {

using SyncVecThreadPoolExecutors =
    folly::Synchronized<std::vector<ThreadPoolExecutor*>>;

SyncVecThreadPoolExecutors& getSyncVecThreadPoolExecutors() {
  static Indestructible<SyncVecThreadPoolExecutors> storage;
  return *storage;
}

DEFINE_int64(
    threadtimeout_ms,
    60000,
    "Idle time before ThreadPoolExecutor threads are joined");

ThreadPoolExecutor::ThreadPoolExecutor(
    size_t /* maxThreads */,
    size_t minThreads,
    std::shared_ptr<ThreadFactory> threadFactory,
    bool isWaitForAll)
    : threadFactory_(std::move(threadFactory)),		// 线程工厂函数，用来创建线程
      isWaitForAll_(isWaitForAll),					// 
      taskStatsCallbacks_(std::make_shared<TaskStatsCallbackRegistry>()),
      threadPoolHook_("folly::ThreadPoolExecutor"),	// 线程池追踪
      minThreads_(minThreads),						// 线程池中最小的线程数
      threadTimeout_(FLAGS_threadtimeout_ms) {		
  // 追踪当前线程池
  getSyncVecThreadPoolExecutors()->push_back(this);
}

ThreadPoolExecutor::~ThreadPoolExecutor() {
  joinKeepAliveOnce();
  CHECK_EQ(0, threadList_.get().size());
  getSyncVecThreadPoolExecutors().withWLock([this](auto& tpe) {
    tpe.erase(std::remove(tpe.begin(), tpe.end(), this), tpe.end());
  });
}

ThreadPoolExecutor::Task::Task(
    Func&& func,
    std::chrono::milliseconds expiration,
    Func&& expireCallback)
    : func_(std::move(func)),
      expiration_(expiration),
      expireCallback_(std::move(expireCallback)),
      context_(folly::RequestContext::saveContext()) {
  // Assume that the task in enqueued on creation
  enqueueTime_ = std::chrono::steady_clock::now();
}

// 在指定线程运行任务task
void ThreadPoolExecutor::runTask(const ThreadPtr& thread, Task&& task) {
  // 空闲标志设置为false
  thread->idle = false;

  // 计算开始时间
  auto startTime = std::chrono::steady_clock::now();

  // 设置task的等待时间 = 开始执行时间 - 入队时间
  task.stats_.waitTime = startTime - task.enqueueTime_;

  // 判断task是否超时
  if (task.expiration_ > std::chrono::milliseconds(0) &&
      task.stats_.waitTime >= task.expiration_) {
	// 设置task的超时标志
	task.stats_.expired = true;

	// 超时回调不为空时调用
    if (task.expireCallback_ != nullptr) {
      task.expireCallback_();
    }
  } else {
    // 没有超时，直接调用
    folly::RequestContextScopeGuard rctx(task.context_);
    try {
      task.func_();
    } catch (const std::exception& e) {
      LOG(ERROR) << "ThreadPoolExecutor: func threw unhandled "
                 << typeid(e).name() << " exception: " << e.what();
    } catch (...) {
      LOG(ERROR) << "ThreadPoolExecutor: func threw unhandled non-exception "
                    "object";
    }
	// 计算task的运行时间 = 结束时间 - 开始执行时间
    task.stats_.runTime = std::chrono::steady_clock::now() - startTime;
  }
  // 线程空闲标志设置为true
  thread->idle = true;

  // 设置上一次执行任务完成的时间
  thread->lastActiveTime = std::chrono::steady_clock::now();

  // 线程执行完每个任务后需要执行的回调
  thread->taskStatsCallbacks->callbackList.withRLock([&](auto& callbacks) {
    // 设置正在执行回调
    *thread->taskStatsCallbacks->inCallback = true;
    SCOPE_EXIT {
      *thread->taskStatsCallbacks->inCallback = false;
    };
    try {
	  // 对于当前完成的任务，调用每个回调
      for (auto& callback : callbacks) {
        callback(task.stats_);
      }
    } catch (const std::exception& e) {
      LOG(ERROR) << "ThreadPoolExecutor: task stats callback threw "
                    "unhandled "
                 << typeid(e).name() << " exception: " << e.what();
    } catch (...) {
      LOG(ERROR) << "ThreadPoolExecutor: task stats callback threw "
                    "unhandled non-exception object";
    }
  });
}

void ThreadPoolExecutor::add(Func, std::chrono::milliseconds, Func) {
  throw std::runtime_error(
      "add() with expiration is not implemented for this Executor");
}

// 返回线程池允许的最大线程数
size_t ThreadPoolExecutor::numThreads() const {
  return maxThreads_.load(std::memory_order_relaxed);
}

// 返回线程池当前的线程数
size_t ThreadPoolExecutor::numActiveThreads() const {
  return activeThreads_.load(std::memory_order_relaxed);
}

// Set the maximum number of running threads.
void ThreadPoolExecutor::setNumThreads(size_t numThreads) {
  /* Since ThreadPoolExecutor may be dynamically adjusting the number of
     threads, we adjust the relevant variables instead of changing
     the number of threads directly.  Roughly:

     If numThreads < minthreads reset minThreads to numThreads.

     If numThreads < active threads, reduce number of running threads.

     If the number of pending tasks is > 0, then increase the currently
     active number of threads such that we can run all the tasks, or reach
     numThreads.

     Note that if there are observers, we actually have to create all
     the threads, because some observer implementations need to 'observe'
     all thread creation (see tests for an example of this)
  */

  size_t numThreadsToJoin = 0;
  {
    // 要动态改变线程数量，先上锁
    SharedMutex::WriteHolder w{&threadListLock_};

	// 获取当前等待执行的任务数
    auto pending = getPendingTaskCountImpl();

	// numThreads是最大的线程数
    maxThreads_.store(numThreads, std::memory_order_relaxed);

	// 获取当前的线程数
    auto active = activeThreads_.load(std::memory_order_relaxed);

	// 获取线程池规定的最小线程数
    auto minthreads = minThreads_.load(std::memory_order_relaxed);

	// 线程池的最小线程数仍然大于目标线程数
	// 减小minthreads_
    if (numThreads < minthreads) {
      minthreads = numThreads;
      minThreads_.store(numThreads, std::memory_order_relaxed);
    }

	// 当前线程数大于目标线程数
	// join掉一定数量的线程
    if (active > numThreads) {
	  // 计算需要join的线程数
      numThreadsToJoin = active - numThreads;
      if (numThreadsToJoin > active - minthreads) {
        numThreadsToJoin = active - minthreads;
      }
	  // 删除一定数量的线程
      ThreadPoolExecutor::removeThreads(numThreadsToJoin, false);

	  // 设置当前线程数
      activeThreads_.store(
          active - numThreadsToJoin, std::memory_order_relaxed);
    } else if (pending > 0 || observers_.size() > 0 || active < minthreads) {
      size_t numToAdd = std::min(pending, numThreads - active);
      if (observers_.size() > 0) {
        numToAdd = numThreads - active;
      }
      if (active + numToAdd < minthreads) {
        numToAdd = minthreads - active;
      }

	  // 增加固定数量的线程
      ThreadPoolExecutor::addThreads(numToAdd);
      activeThreads_.store(active + numToAdd, std::memory_order_relaxed);
    }
  }

  /* We may have removed some threads, attempt to join them */
  joinStoppedThreads(numThreadsToJoin);
}

// threadListLock_ is writelocked

// 在线程池中增加n个线程
void ThreadPoolExecutor::addThreads(size_t n) {
  std::vector<ThreadPtr> newThreads;

  // 创建n个线程wrapper，保存到容器中
  for (size_t i = 0; i < n; i++) {
    newThreads.push_back(makeThread());
  }
  for (auto& thread : newThreads) {
    // TODO need a notion of failing to create the thread
    // and then handling for that case

	// 对于每个wrapper，创建一个新的线程与之对应
	// 每个线程执行threadRun函数，CPU/IO ThreadPool会自定义实现
    thread->handle = threadFactory_->newThread(
        std::bind(&ThreadPoolExecutor::threadRun, this, thread));

	// 线程列表中增加线程wrapper
    threadList_.add(thread);
  }

  // 线程wrapper中包含
  // 1. thread对象
  // 2. 信号量
  // 这里的作用是等待n个线程都创建完毕(每个线程在进入threadRun时会post自己的信号量)
  for (auto& thread : newThreads) {
    thread->startupBaton.wait();
  }

  // 添加观察者
  for (auto& o : observers_) {
    for (auto& thread : newThreads) {
      o->threadStarted(thread.get());
    }
  }
}

// threadListLock_ is writelocked
void ThreadPoolExecutor::removeThreads(size_t n, bool isJoin) {
  isJoin_ = isJoin;
  stopThreads(n);
}

// 已经停掉的线程保存在stoppedThreads_中，调用这个函数join掉
void ThreadPoolExecutor::joinStoppedThreads(size_t n) {
  for (size_t i = 0; i < n; i++) {
    auto thread = stoppedThreads_.take();
    thread->handle.join();
  }
}

void ThreadPoolExecutor::stop() {
  joinKeepAliveOnce();
  size_t n = 0;
  {
    SharedMutex::WriteHolder w{&threadListLock_};
	// 清零
	maxThreads_.store(0, std::memory_order_release);
    activeThreads_.store(0, std::memory_order_release);

	// 当前线程数
    n = threadList_.get().size();

	// 移除n个线程，将其加入到stopThreads_中
    removeThreads(n, false);

	// 需要join的线程数
    n += threadsToJoin_.load(std::memory_order_relaxed);

	// 清零
	threadsToJoin_.store(0, std::memory_order_relaxed);
  }
  // join掉已经停止的线程
  joinStoppedThreads(n);
  CHECK_EQ(0, threadList_.get().size());
  CHECK_EQ(0, stoppedThreads_.size());
}

void ThreadPoolExecutor::join() {
  joinKeepAliveOnce();
  size_t n = 0;
  {
    SharedMutex::WriteHolder w{&threadListLock_};
    maxThreads_.store(0, std::memory_order_release);
    activeThreads_.store(0, std::memory_order_release);
    n = threadList_.get().size();
    removeThreads(n, true);
    n += threadsToJoin_.load(std::memory_order_relaxed);
    threadsToJoin_.store(0, std::memory_order_relaxed);
  }
  joinStoppedThreads(n);
  CHECK_EQ(0, threadList_.get().size());
  CHECK_EQ(0, stoppedThreads_.size());
}

void ThreadPoolExecutor::withAll(FunctionRef<void(ThreadPoolExecutor&)> f) {
  getSyncVecThreadPoolExecutors().withRLock([f](auto& tpes) {
    for (auto tpe : tpes) {
      f(*tpe);
    }
  });
}

ThreadPoolExecutor::PoolStats ThreadPoolExecutor::getPoolStats() const {
  const auto now = std::chrono::steady_clock::now();
  SharedMutex::ReadHolder r{&threadListLock_};
  ThreadPoolExecutor::PoolStats stats;
  size_t activeTasks = 0;
  size_t idleAlive = 0;
  for (auto thread : threadList_.get()) {
    if (thread->idle) {
      const std::chrono::nanoseconds idleTime = now - thread->lastActiveTime;
      stats.maxIdleTime = std::max(stats.maxIdleTime, idleTime);
      idleAlive++;
    } else {
      activeTasks++;
    }
  }
  stats.pendingTaskCount = getPendingTaskCountImpl();
  stats.totalTaskCount = stats.pendingTaskCount + activeTasks;

  stats.threadCount = maxThreads_.load(std::memory_order_relaxed);
  stats.activeThreadCount =
      activeThreads_.load(std::memory_order_relaxed) - idleAlive;
  stats.idleThreadCount = stats.threadCount - stats.activeThreadCount;
  return stats;
}

size_t ThreadPoolExecutor::getPendingTaskCount() const {
  SharedMutex::ReadHolder r{&threadListLock_};
  return getPendingTaskCountImpl();
}

std::string ThreadPoolExecutor::getName() const {
  auto ntf = dynamic_cast<NamedThreadFactory*>(threadFactory_.get());
  if (ntf == nullptr) {
    return folly::demangle(typeid(*this).name()).toStdString();
  }

  return ntf->getNamePrefix();
}

std::atomic<uint64_t> ThreadPoolExecutor::Thread::nextId(0);

void ThreadPoolExecutor::subscribeToTaskStats(TaskStatsCallback cb) {
  if (*taskStatsCallbacks_->inCallback) {
    throw std::runtime_error("cannot subscribe in task stats callback");
  }
  taskStatsCallbacks_->callbackList.wlock()->push_back(std::move(cb));
}

BlockingQueueAddResult ThreadPoolExecutor::StoppedThreadQueue::add(
    ThreadPoolExecutor::ThreadPtr item) {
  std::lock_guard<std::mutex> guard(mutex_);
  queue_.push(std::move(item));
  return sem_.post();
}

ThreadPoolExecutor::ThreadPtr ThreadPoolExecutor::StoppedThreadQueue::take() {
  while (true) {
    {
      std::lock_guard<std::mutex> guard(mutex_);
      if (queue_.size() > 0) {
        auto item = std::move(queue_.front());
        queue_.pop();
        return item;
      }
    }
    sem_.wait();
  }
}

folly::Optional<ThreadPoolExecutor::ThreadPtr>
ThreadPoolExecutor::StoppedThreadQueue::try_take_for(
    std::chrono::milliseconds time) {
  while (true) {
    {
      std::lock_guard<std::mutex> guard(mutex_);
      if (queue_.size() > 0) {
        auto item = std::move(queue_.front());
        queue_.pop();
        return item;
      }
    }
    if (!sem_.try_wait_for(time)) {
      return folly::none;
    }
  }
}

size_t ThreadPoolExecutor::StoppedThreadQueue::size() {
  std::lock_guard<std::mutex> guard(mutex_);
  return queue_.size();
}

void ThreadPoolExecutor::addObserver(std::shared_ptr<Observer> o) {
  {
    SharedMutex::WriteHolder r{&threadListLock_};
    observers_.push_back(o);
    for (auto& thread : threadList_.get()) {
      o->threadPreviouslyStarted(thread.get());
    }
  }
  while (activeThreads_.load(std::memory_order_relaxed) <
         maxThreads_.load(std::memory_order_relaxed)) {
    ensureActiveThreads();
  }
}

void ThreadPoolExecutor::removeObserver(std::shared_ptr<Observer> o) {
  SharedMutex::WriteHolder r{&threadListLock_};
  for (auto& thread : threadList_.get()) {
    o->threadNotYetStopped(thread.get());
  }

  for (auto it = observers_.begin(); it != observers_.end(); it++) {
    if (*it == o) {
      observers_.erase(it);
      return;
    }
  }
  DCHECK(false);
}

// Idle threads may have destroyed themselves, attempt to join
// them here

// join掉退出的线程
void ThreadPoolExecutor::ensureJoined() {
  // 获取退出的线程数
  auto tojoin = threadsToJoin_.load(std::memory_order_relaxed);
  if (tojoin) {
    {
      // 加锁，解决ABA问题
      SharedMutex::WriteHolder w{&threadListLock_};
      tojoin = threadsToJoin_.load(std::memory_order_relaxed);
      threadsToJoin_.store(0, std::memory_order_relaxed);
    }
    // join掉一定数量的线程
    joinStoppedThreads(tojoin);
  }
}

// threadListLock_ must be write locked.
bool ThreadPoolExecutor::tryTimeoutThread() {
  // Try to stop based on idle thread timeout (try_take_for),
  // if there are at least minThreads running.
  if (!minActive()) {
    return false;
  }

  // Remove thread from active count
  activeThreads_.store(
      activeThreads_.load(std::memory_order_relaxed) - 1,
      std::memory_order_relaxed);

  // There is a memory ordering constraint w.r.t the queue
  // implementation's add() and getPendingTaskCountImpl() - while many
  // queues have seq_cst ordering, some do not, so add an explicit
  // barrier.  tryTimeoutThread is the slow path and only happens once
  // every thread timeout; use asymmetric barrier to keep add() fast.
  asymmetricHeavyBarrier();

  // If this is based on idle thread timeout, then
  // adjust vars appropriately (otherwise stop() or join()
  // does this).
  if (getPendingTaskCountImpl() > 0) {
    // There are still pending tasks, we can't stop yet.
    // re-up active threads and return.
    activeThreads_.store(
        activeThreads_.load(std::memory_order_relaxed) + 1,
        std::memory_order_relaxed);
    return false;
  }

  threadsToJoin_.store(
      threadsToJoin_.load(std::memory_order_relaxed) + 1,
      std::memory_order_relaxed);

  return true;
}

// If we can't ensure that we were able to hand off a task to a thread,
// attempt to start a thread that handled the task, if we aren't already
// running the maximum number of threads.
void ThreadPoolExecutor::ensureActiveThreads() {

  // join掉已经退出的线程
  ensureJoined();

  // Matches barrier in tryTimeoutThread().  Ensure task added
  // is seen before loading activeThreads_ below.
  asymmetricLightBarrier();

  // Fast path assuming we are already at max threads.

  // 获取当前线程数和最大允许的线程数
  auto active = activeThreads_.load(std::memory_order_relaxed);
  auto total = maxThreads_.load(std::memory_order_relaxed);

  // 已经达到最大值，不能再创建线程了，退出
  if (active >= total) {
    return;
  }

  SharedMutex::WriteHolder w{&threadListLock_};
  // Double check behind lock.

  // ABA问题
  active = activeThreads_.load(std::memory_order_relaxed);
  total = maxThreads_.load(std::memory_order_relaxed);
  if (active >= total) {
    return;
  }

  // 增加一个线程
  ThreadPoolExecutor::addThreads(1);
  activeThreads_.store(active + 1, std::memory_order_relaxed);
}

// If an idle thread times out, only join it if there are at least
// minThreads threads.
bool ThreadPoolExecutor::minActive() {
  return activeThreads_.load(std::memory_order_relaxed) >
      minThreads_.load(std::memory_order_relaxed);
}

} // namespace folly
