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

#include <folly/executors/CPUThreadPoolExecutor.h>

#include <folly/executors/task_queue/PriorityLifoSemMPMCQueue.h>
#include <folly/portability/GFlags.h>

DEFINE_bool(
    dynamic_cputhreadpoolexecutor,
    true,
    "CPUThreadPoolExecutor will dynamically create and destroy threads");

namespace folly {

const size_t CPUThreadPoolExecutor::kDefaultMaxQueueSize = 1 << 14;

CPUThreadPoolExecutor::CPUThreadPoolExecutor(
    size_t numThreads,
    std::unique_ptr<BlockingQueue<CPUTask>> taskQueue,
    std::shared_ptr<ThreadFactory> threadFactory)
    : ThreadPoolExecutor(
          numThreads,
          FLAGS_dynamic_cputhreadpoolexecutor ? 0 : numThreads,
          std::move(threadFactory)),
      taskQueue_(std::move(taskQueue)) {
  setNumThreads(numThreads);
}

CPUThreadPoolExecutor::CPUThreadPoolExecutor(
    std::pair<size_t, size_t> numThreads,
    std::unique_ptr<BlockingQueue<CPUTask>> taskQueue,
    std::shared_ptr<ThreadFactory> threadFactory)
    : ThreadPoolExecutor(
          numThreads.first,
          numThreads.second,
          std::move(threadFactory)),
      taskQueue_(std::move(taskQueue)) {
  setNumThreads(numThreads.first);
}

CPUThreadPoolExecutor::CPUThreadPoolExecutor(
    size_t numThreads,
    std::shared_ptr<ThreadFactory> threadFactory)
    : CPUThreadPoolExecutor(
          numThreads,
          std::make_unique<LifoSemMPMCQueue<CPUTask>>(
              CPUThreadPoolExecutor::kDefaultMaxQueueSize),
          std::move(threadFactory)) {}

CPUThreadPoolExecutor::CPUThreadPoolExecutor(
    std::pair<size_t, size_t> numThreads,
    std::shared_ptr<ThreadFactory> threadFactory)
    : CPUThreadPoolExecutor(
          numThreads,
          std::make_unique<LifoSemMPMCQueue<CPUTask>>(
              CPUThreadPoolExecutor::kDefaultMaxQueueSize),
          std::move(threadFactory)) {}

CPUThreadPoolExecutor::CPUThreadPoolExecutor(size_t numThreads)
    : CPUThreadPoolExecutor(
          numThreads,
          std::make_shared<NamedThreadFactory>("CPUThreadPool")) {}

CPUThreadPoolExecutor::CPUThreadPoolExecutor(
    size_t numThreads,
    int8_t numPriorities,
    std::shared_ptr<ThreadFactory> threadFactory)
    : CPUThreadPoolExecutor(
          numThreads,
          std::make_unique<PriorityLifoSemMPMCQueue<CPUTask>>(
              numPriorities,
              CPUThreadPoolExecutor::kDefaultMaxQueueSize),
          std::move(threadFactory)) {}

CPUThreadPoolExecutor::CPUThreadPoolExecutor(
    size_t numThreads,
    int8_t numPriorities,
    size_t maxQueueSize,
    std::shared_ptr<ThreadFactory> threadFactory)
    : CPUThreadPoolExecutor(
          numThreads,
          std::make_unique<PriorityLifoSemMPMCQueue<CPUTask>>(
              numPriorities,
              maxQueueSize),
          std::move(threadFactory)) {}

CPUThreadPoolExecutor::~CPUThreadPoolExecutor() {
  stop();
  CHECK(threadsToStop_ == 0);
}

void CPUThreadPoolExecutor::add(Func func) {
  add(std::move(func), std::chrono::milliseconds(0));
}

void CPUThreadPoolExecutor::add(
    Func func,
    std::chrono::milliseconds expiration,
    Func expireCallback) {

  // taskQueue_: 阻塞队列，线程在threadRun中会阻塞take任务
  // 相当于生产者消费者队列
  auto result = taskQueue_->add(
      CPUTask(std::move(func), expiration, std::move(expireCallback)));
  if (!result.reusedThread) {
    ensureActiveThreads();
  }
}

void CPUThreadPoolExecutor::addWithPriority(Func func, int8_t priority) {
  add(std::move(func), priority, std::chrono::milliseconds(0));
}

void CPUThreadPoolExecutor::add(
    Func func,
    int8_t priority,
    std::chrono::milliseconds expiration,
    Func expireCallback) {
  CHECK(getNumPriorities() > 0);
  auto result = taskQueue_->addWithPriority(
      CPUTask(std::move(func), expiration, std::move(expireCallback)),
      priority);
  if (!result.reusedThread) {
    ensureActiveThreads();
  }
}

uint8_t CPUThreadPoolExecutor::getNumPriorities() const {
  return taskQueue_->getNumPriorities();
}

size_t CPUThreadPoolExecutor::getTaskQueueSize() const {
  return taskQueue_->size();
}

BlockingQueue<CPUThreadPoolExecutor::CPUTask>*
CPUThreadPoolExecutor::getTaskQueue() {
  return taskQueue_.get();
}

// threadListLock_ must be writelocked.

// 调用这个函数之前，threadListLock_需要被锁住，否则memeory_order_relaxed屏障过于宽松会有问题
bool CPUThreadPoolExecutor::tryDecrToStop() {
  auto toStop = threadsToStop_.load(std::memory_order_relaxed);
  if (toStop <= 0) {
    return false;
  }
  threadsToStop_.store(toStop - 1, std::memory_order_relaxed);
  return true;
}

bool CPUThreadPoolExecutor::taskShouldStop(folly::Optional<CPUTask>& task) {
  if (tryDecrToStop()) {
    return true;
  }
  if (task) {
    return false;
  } else {
    return tryTimeoutThread();
  }
  return true;
}

// 覆盖基类ThreadPoolExecutor::threadRun的纯虚函数
// 创建一个新的线程时，这个线程会调用该函数
void CPUThreadPoolExecutor::threadRun(ThreadPtr thread) {
  this->threadPoolHook_.registerThread();

  // 表示当前线程开始工作
  thread->startupBaton.post();

  // 无限循环，从任务队列中取出任务并执行
  while (true) {

    // taskQueue_: 阻塞队列
    auto task = taskQueue_->try_take_for(threadTimeout_);
    // Handle thread stopping, either by task timeout, or
    // by 'poison' task added in join() or stop().
    
    if (UNLIKELY(!task || task.value().poison)) {
      // Actually remove the thread from the list.
      SharedMutex::WriteHolder w{&threadListLock_};

	  // 某一特殊类型的task，用于提示线程退出
      if (taskShouldStop(task)) {

	    // 调用观察者线程退出函数
        for (auto& o : observers_) {
          o->threadStopped(thread.get());
        }
		// 将当前线程从线程列表中删除
        threadList_.remove(thread);

		// 当前线程退出，放到stoppedThreads_中，等待被join
        stoppedThreads_.add(thread);
        return;
      } else {
        continue;
      }
    }

	// 在当前线程运行任务task
    runTask(thread, std::move(task.value()));

	// 如果需要退出的线程数非零，则尝试退出当前线程
    if (UNLIKELY(threadsToStop_ > 0 && !isJoin_)) {
      SharedMutex::WriteHolder w{&threadListLock_};

	  // 当前线程获取到了退出名额
      if (tryDecrToStop()) {
        threadList_.remove(thread);
        stoppedThreads_.add(thread);
        return;
      }
    }
  }
}

// 停止n个线程，向阻塞队列中添加n个空任务，提示获取到这种类型任务的线程退出
void CPUThreadPoolExecutor::stopThreads(size_t n) {
  threadsToStop_ += n;
  for (size_t i = 0; i < n; i++) {
    taskQueue_->addWithPriority(CPUTask(), Executor::LO_PRI);
  }
}

// threadListLock_ is read (or write) locked.
// 阻塞队列中的任务数就是等待被执行的任务数
size_t CPUThreadPoolExecutor::getPendingTaskCountImpl() const {
  return taskQueue_->size();
}

} // namespace folly
