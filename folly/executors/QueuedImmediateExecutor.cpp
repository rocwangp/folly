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

#include <folly/executors/QueuedImmediateExecutor.h>

#include <folly/Indestructible.h>

namespace folly {

QueuedImmediateExecutor& QueuedImmediateExecutor::instance() {
  static auto instance = Indestructible<QueuedImmediateExecutor>{};
  return *instance;
}

// 类似InlineExecutor，区别是不会递归调用
// 如果func中递归调用了add(func1)
// 对于InlineExecutor，会在func中调用func1
// 对于QueuedImmediateExecutr，会先调用func再调用func1
void QueuedImmediateExecutor::add(Func callback) {
  auto& q = *q_;
  q.push(std::move(callback));

  // q.size() == 1标识第一次进入add而不是q.front()()的第n次进入
  if (q.size() == 1) {
    while (!q.empty()) {
      q.front()();
      q.pop();
    }
  }
}

} // namespace folly
