/*
 * Copyright 2014-present Facebook, Inc.
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

#include <folly/executors/ManualExecutor.h>

#include <string.h>
#include <string>
#include <tuple>

namespace folly {

// 析构函数会处理完所有的callback
ManualExecutor::~ManualExecutor() {
  drain();
}

// 将callback添加到任务队列中
void ManualExecutor::add(Func callback) {
  // 上锁
  // lock_guard和unique_lock的区别
  // lock_guard只有RAII语义
  // unique_lock除了RAII语义外，还可以手动解锁，常用于condition_variable配合使用
  std::lock_guard<std::mutex> lock(lock_);

  // 将callback添加到任务队列中
  funcs_.emplace(std::move(callback));

  // 通知调度侧处理任务队列中的callback
  sem_.post();
}

// 处理任务队列中的callback
size_t ManualExecutor::run() {
  size_t count;
  size_t n;
  Func func;

  {
    // 上锁
    std::lock_guard<std::mutex> lock(lock_);

	// scheduledFuncs_用于处理超时任务
	// priority_queue利用最小堆的原理，使得堆顶的任务距离超时最近
    while (!scheduledFuncs_.empty()) {
      auto& sf = scheduledFuncs_.top();
      if (sf.time > now_) {
        break;
      }
      funcs_.emplace(sf.moveOutFunc());
      scheduledFuncs_.pop();
    }

	// 获取当前视图下的任务数，也只处理这些callback
	// 也就是说如果funcs_中的func递归调用了ManualExecutor::add，如func1，这个func1是不会被调用的
    n = funcs_.size();
  }

  for (count = 0; count < n; count++) {
    {
      // 上锁
      std::lock_guard<std::mutex> lock(lock_);
      if (funcs_.empty()) {
        break;
      }

      // Balance the semaphore so it doesn't grow without bound
      // if nobody is calling wait().
      // This may fail (with EAGAIN), that's fine.

	  // 等待add中的sem_.post()唤醒
	  sem_.tryWait();

	  // 从任务队列中取出任务并执行
      func = std::move(funcs_.front());

	  // 弹出第一个任务
      funcs_.pop();
    }

    // 执行
    func();
  }

  return count;
}

// 不断执行任务队列中的任务，直到任务队列为空
// 通常如果任务队列中的某个任务func递归调用的Executor::add(func1)
// 那么在run()中func1是不会被执行的，而drain会执行
size_t ManualExecutor::drain() {
  size_t tasksRun = 0;
  size_t tasksForSingleRun = 0;
  while ((tasksForSingleRun = run()) != 0) {
    tasksRun += tasksForSingleRun;
  }
  return tasksRun;
}

void ManualExecutor::wait() {
  while (true) {
    {
      std::lock_guard<std::mutex> lock(lock_);
      if (!funcs_.empty()) {
        break;
      }
    }

    sem_.wait();
  }
}

// 设置执行器的时钟，然后重新执行run，这样子才有可能会有超时任务被执行
void ManualExecutor::advanceTo(TimePoint const& t) {
  if (t > now_) {
    now_ = t;
  }
  run();
}

} // namespace folly
