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

#include <folly/executors/ThreadedExecutor.h>

#include <chrono>

#include <glog/logging.h>

#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <folly/system/ThreadName.h>

namespace folly {

template <typename F>
static auto with_unique_lock(std::mutex& m, F&& f) -> decltype(f()) {
  std::unique_lock<std::mutex> lock(m);
  return f();
}

ThreadedExecutor::ThreadedExecutor(std::shared_ptr<ThreadFactory> threadFactory)
    : threadFactory_(std::move(threadFactory)) {
  // 创建一个单独的后台线程用来执行add增加的callback
  controlt_ = std::thread([this] { control(); });
}

// 析构函数会唤醒控制线程，清空任务队列，join掉
ThreadedExecutor::~ThreadedExecutor() {
  stopping_.store(true, std::memory_order_release);
  notify();
  controlt_.join();
  CHECK(running_.empty());
  CHECK(finished_.empty());
}

// 将callback添加到任务队列中等待被执行
void ThreadedExecutor::add(Func func) {
  CHECK(!stopping_.load(std::memory_order_acquire));
  // 获取互斥锁enqueuedm_，然后将callback添加到任务队列中，最后解锁
  with_unique_lock(enqueuedm_, [&] { enqueued_.push_back(std::move(func)); });
  // 唤醒控制线程开始处理任务队列中的任务
  notify();
}

std::shared_ptr<ThreadFactory> ThreadedExecutor::newDefaultThreadFactory() {
  return std::make_shared<NamedThreadFactory>("Threaded");
}

void ThreadedExecutor::notify() {
  // 获取控制锁，设置控制线程运行标志为true
  with_unique_lock(controlm_, [&] { controls_ = true; });

  // 唤醒后台控制线程
  controlc_.notify_one();
}


// 后台线程执行函数，循环处理每个任务
void ThreadedExecutor::control() {
  folly::setThreadName("ThreadedCtrl");
  auto looping = true;
  while (looping) {
  	// 等待notify唤醒当前控制线程
    controlWait();

    // 处理任务队列中的任务
    // 只要stop不为true，或者还有正在运行的工作线程，就继续wait
    looping = controlPerformAll();
  }
}

// 等待条件变量被唤醒或超时
void ThreadedExecutor::controlWait() {
  constexpr auto kMaxWait = std::chrono::seconds(10);
  std::unique_lock<std::mutex> lock(controlm_);
  controlc_.wait_for(lock, kMaxWait, [&] { return controls_; });
  controls_ = false;
}

// 执行callback的线程逻辑
void ThreadedExecutor::work(Func& func) {
  func();
  auto id = std::this_thread::get_id();
  // callback执行完成，将当前线程id放到finished_中
  // finished_被控制线程和任务执行线程共享，存在竞争，需要锁保护
  with_unique_lock(finishedm_, [&] { finished_.push_back(id); });

  // 唤醒控制线程
  notify();
}

// 将已经执行完的线程id清理掉
void ThreadedExecutor::controlJoinFinishedThreads() {
  std::deque<std::thread::id> finishedt;
  // 获取finishedm_锁，将保有已完成线程id的队列换出
  // finished_被控制线程和任务执行线程共享，存在竞争，需要锁保护
  with_unique_lock(finishedm_, [&] { std::swap(finishedt, finished_); });

  // 对于队列中的每个线程id，先join后从running_ map中删除
  // running_只被控制线程持有，不存在竞争，不需要锁保护
  for (auto id : finishedt) {
    running_[id].join();
    running_.erase(id);
  }
}

void ThreadedExecutor::controlLaunchEnqueuedTasks() {
  std::deque<Func> enqueuedt;
  // 获取任务队列中的任务
  // enqueued_被add调用线程和控制线程共享，存在竞争，需要锁保护
  with_unique_lock(enqueuedm_, [&] { std::swap(enqueuedt, enqueued_); });

  // 对于任务队列中的每个任务，创建一个新线程去处理
  for (auto& f : enqueuedt) {
    auto th = threadFactory_->newThread(
        [this, f = std::move(f)]() mutable { work(f); });
	// 将创建的线程id保存，等待下次由控制线程join掉
    auto id = th.get_id();
    running_[id] = std::move(th);
  }
}

// 处理当前任务队列中的callback
bool ThreadedExecutor::controlPerformAll() {
  auto stopping = stopping_.load(std::memory_order_acquire);
  controlJoinFinishedThreads();
  controlLaunchEnqueuedTasks();
  return !stopping || !running_.empty();
}
} // namespace folly
