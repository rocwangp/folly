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

#include <folly/io/async/HHWheelTimer.h>

#include <cassert>

#include <folly/Memory.h>
#include <folly/Optional.h>
#include <folly/ScopeGuard.h>
#include <folly/container/BitIterator.h>
#include <folly/io/async/Request.h>
#include <folly/lang/Bits.h>


namespace folly {
/**
 * We want to select the default interval carefully.
 * An interval of 10ms will give us 10ms * WHEEL_SIZE^WHEEL_BUCKETS
 * for the largest timeout possible, or about 497 days.
 *
 * For a lower bound, we want a reasonable limit on local IO, 10ms
 * seems short enough
 *
 * A shorter interval also has CPU implications, less than 1ms might
 * start showing up in cpu perf.  Also, it might not be possible to set
 * tick interval less than 10ms on older kernels.
 */

/*
 * For high res timers:
 * An interval of 200usec will give us 200usec * WHEEL_SIZE^WHEEL_BUCKETS
 * for the largest timeout possible, or about 9 days.
 */

template <class Duration>
int HHWheelTimerBase<Duration>::DEFAULT_TICK_INTERVAL =
    detail::HHWheelTimerDurationConst<Duration>::DEFAULT_TICK_INTERVAL;

template <class Duration>
HHWheelTimerBase<Duration>::Callback::Callback() {}

template <class Duration>
HHWheelTimerBase<Duration>::Callback::~Callback() {
  if (isScheduled()) {
    cancelTimeout();
  }
}

template <class Duration>
void HHWheelTimerBase<Duration>::Callback::setScheduled(
    HHWheelTimerBase* wheel,
    std::chrono::steady_clock::time_point deadline) {
  assert(wheel_ == nullptr);
  assert(expiration_ == decltype(expiration_){});

  wheel_ = wheel;
  expiration_ = deadline;
}

template <class Duration>
void HHWheelTimerBase<Duration>::Callback::cancelTimeoutImpl() {
  if (--wheel_->count_ <= 0) {
    assert(wheel_->count_ == 0);
    wheel_->AsyncTimeout::cancelTimeout();
  }
  unlink();
  if ((-1 != bucket_) && (wheel_->buckets_[0][bucket_].empty())) {
    auto bi = makeBitIterator(wheel_->bitmap_.begin());
    *(bi + bucket_) = false;
  }

  wheel_ = nullptr;
  expiration_ = {};
}

template <class Duration>
HHWheelTimerBase<Duration>::HHWheelTimerBase(
    folly::TimeoutManager* timeoutMananger,
    Duration intervalDuration,
    AsyncTimeout::InternalEnum internal,
    Duration defaultTimeoutDuration)
    : AsyncTimeout(timeoutMananger, internal),
      interval_(intervalDuration),
      defaultTimeout_(defaultTimeoutDuration),
      expireTick_(1),
      count_(0),
      startTime_(getCurTime()),
      processingCallbacksGuard_(nullptr) {
  bitmap_.fill(0);
}

template <class Duration>
HHWheelTimerBase<Duration>::~HHWheelTimerBase() {
  // Ensure this gets done, but right before destruction finishes.
  auto destructionPublisherGuard = folly::makeGuard([&] {
    // Inform the subscriber that this instance is doomed.
    if (processingCallbacksGuard_) {
      *processingCallbacksGuard_ = true;
    }
  });
  cancelAll();
}

// 将定时任务添加到时间轮中
// dueTick: 定时任务超时的时间轮刻度数
// nextTickToProcess: 下一个要到达的时间轮刻度
// nextTick: 当前时刻对应的时间轮刻度数

// 时间轮的刻度盘从某一时刻开始运转，这个开始时间作为所有超时任务时间的参考系
// 所以任何一个超时时间就都可以在刻度盘上找到对应的位置
template <class Duration>
void HHWheelTimerBase<Duration>::scheduleTimeoutImpl(
    Callback* callback,
    int64_t dueTick,
    int64_t nextTickToProcess,
    int64_t nextTick) {

  // 当前时刻到定时器超时的相对刻度数
  int64_t diff = dueTick - nextTickToProcess;
  CallbackList* list;

  auto bi = makeBitIterator(bitmap_.begin());

  // 每个刻度盘表示一定范围的刻度数
  // buckets_[0]: 1 ~ (1 << 8)
  // buckets_[1]: (1 << 8) ~ (1 << 16) => (1 ~ (1 << 8)) << 8
  // buckets_[2]: (1 << 16) ~ (1 << 24) => (1 ~ (1 << 8)) << 8 << 8
  // buckets_[0]每移动一圈，buckets_[1]移动一个刻度
  // buckets_[1]每移动一圈，buckets_[2]移动一个刻度

  // 对于某个定时任务的绝对时间，可以在时间轮刻度盘上找到
  // 一组<buckets_[2][a], buckets_[1][b], buckets_[0][c]>
  // 先将它放到a上面，刻度到达时放到b上面，刻度到达时放到c上面，之后若刻度到达则代表超时
  if (diff < 0) {
    list = &buckets_[0][nextTick & WHEEL_MASK];
    *(bi + (nextTick & WHEEL_MASK)) = true;
    callback->bucket_ = nextTick & WHEEL_MASK;
  // 距离超时的刻度数小于buckets_[0]一圈，那直接添加到buckets_[0]即可
  // 等到下次移动到对应刻度位置，就可以认为超时
  // dueTick为超时任务的绝对刻度数，映射到第一维度即可
  } else if (diff < WHEEL_SIZE) {
    list = &buckets_[0][dueTick & WHEEL_MASK];
    *(bi + (dueTick & WHEEL_MASK)) = true;
    callback->bucket_ = dueTick & WHEEL_MASK;
  // 距离超时的刻度数大于buckets_[0]一圈，但是小于buckets_[1]一圈
  // 添加到buckets_[1]上，将任务投影到buckets_[1]
  // 等到指针指向对应刻度，再将其映射到buckets_[0]上
  } else if (diff < 1 << (2 * WHEEL_BITS)) {
    list = &buckets_[1][(dueTick >> WHEEL_BITS) & WHEEL_MASK];
  // 同理，先映射到buckets_[2]上，再映射到buckets_[1]，最后映射到buckets_[0]
  } else if (diff < 1 << (3 * WHEEL_BITS)) {
    list = &buckets_[2][(dueTick >> 2 * WHEEL_BITS) & WHEEL_MASK];
  // 同理，先映射到buckets_[3]上，再映射到buckets_[2]，之后映射到buckets_[1], 最后映射到buckets_[0]
  } else {
    /* in largest slot */
    if (diff > LARGEST_SLOT) {
      diff = LARGEST_SLOT;
      dueTick = diff + nextTickToProcess;
    }
    list = &buckets_[3][(dueTick >> 3 * WHEEL_BITS) & WHEEL_MASK];
  }

  // 每个buckets_[a][b]是一个链表，表示对应超时任务
  // 上述步骤找到对应的链表，将超时任务添加进去
  list->push_back(*callback);
}

// 添加一个定时任务到时间轮
template <class Duration>
void HHWheelTimerBase<Duration>::scheduleTimeout(
    Callback* callback,
    Duration timeout) {
  // Make sure that the timeout is not negative.
  timeout = std::max(timeout, Duration::zero());
  // Cancel the callback if it happens to be scheduled already.
  callback->cancelTimeout();
  callback->requestContext_ = RequestContext::saveContext();

  // 定时任务数
  count_++;

  // 当前时间
  auto now = getCurTime(); 

  // 将当前时间表示为时间轮上的刻度
  auto nextTick = calcNextTick(now);

  // 为回调函数设置超时时间
  callback->setScheduled(this, now + timeout);

  // There are three possible scenarios:
  //   - we are currently inside of HHWheelTimerBase<Duration>::timeoutExpired.
  //   In this case,
  //     we need to use its last tick as a base for computations
  //   - HHWheelTimerBase tick timeout is already scheduled. In this case,
  //     we need to use its scheduled tick as a base.
  //   - none of the above are true. In this case, it's safe to use the nextTick
  //     as a base.

  // 当前时刻对应的时间轮刻度
  int64_t baseTick = nextTick;
  if (processingCallbacksGuard_ || isScheduled()) {
    baseTick = std::min(expireTick_, nextTick);
  }

  // 距离超时还需要走多少个时间轮刻度
  int64_t ticks = timeToWheelTicks(timeout);

  // 定时任务超时对应的绝对刻度数
  int64_t due = ticks + nextTick;

  // 将定时任务添加到时间轮中
  scheduleTimeoutImpl(callback, due, baseTick, nextTick);

  /* If we're calling callbacks, timer will be reset after all
   * callbacks are called.
   */
  if (!processingCallbacksGuard_) {
    // Check if we need to reschedule the timer.
    // If the wheel timeout is already scheduled, then we need to reschedule
    // only if our due is earlier than the current scheduled one.
    // If it's not scheduled, we need to schedule it either for the first tick
    // of next wheel epoch or our due tick, whichever is earlier.
    if (!isScheduled() && !inSameEpoch(nextTick - 1, due)) {
      scheduleNextTimeout(nextTick, WHEEL_SIZE - ((nextTick - 1) & WHEEL_MASK));
    } else if (!isScheduled() || due < expireTick_) {
      scheduleNextTimeout(nextTick, ticks + 1);
    }
  }
}

template <class Duration>
void HHWheelTimerBase<Duration>::scheduleTimeout(Callback* callback) {
  CHECK(Duration(-1) != defaultTimeout_)
      << "Default timeout was not initialized";
  scheduleTimeout(callback, defaultTimeout_);
}

template <class Duration>
bool HHWheelTimerBase<Duration>::cascadeTimers(int bucket, int tick) {
  CallbackList cbs;
  cbs.swap(buckets_[bucket][tick]);
  auto now = getCurTime();
  auto nextTick = calcNextTick(now);
  while (!cbs.empty()) {
    auto* cb = &cbs.front();
    cbs.pop_front();
    scheduleTimeoutImpl(
        cb,
        nextTick + timeToWheelTicks(cb->getTimeRemaining(now)),
        expireTick_,
        nextTick);
  }

  // If tick is zero, timeoutExpired will cascade the next bucket.
  return tick == 0;
}

template <class Duration>
void HHWheelTimerBase<Duration>::scheduleTimeoutInternal(Duration timeout) {
  this->AsyncTimeout::scheduleTimeout(timeout);
}

// 移动时间轮刻度盘，将超时任务取出
// 先移动buckets_[0]，如果完成一轮，则转动buckets_[1]，如果完成一轮，转到buckets_[2]...
// 每转动一个高层buckets_，就将任务取出减去对应的刻度数，放到底层的buckets_中
template <class Duration>
void HHWheelTimerBase<Duration>::timeoutExpired() noexcept {

  // 计算当前时刻对应的时间轮刻度
  auto nextTick = calcNextTick();

  // If the last smart pointer for "this" is reset inside the callback's
  // timeoutExpired(), then the guard will detect that it is time to bail from
  // this method.
  auto isDestroyed = false;
  // If scheduleTimeout is called from a callback in this function, it may
  // cause inconsistencies in the state of this object. As such, we need
  // to treat these calls slightly differently.
  CHECK(!processingCallbacksGuard_);
  processingCallbacksGuard_ = &isDestroyed;
  auto reEntryGuard = folly::makeGuard([&] {
    if (!isDestroyed) {
      processingCallbacksGuard_ = nullptr;
    }
  });

  // timeoutExpired() can only be invoked directly from the event base loop.
  // It should never be invoked recursively.
  //
  while (expireTick_ < nextTick) {
    int idx = expireTick_ & WHEEL_MASK;

    if (idx == 0) {
      // Cascade timers
      if (cascadeTimers(1, (expireTick_ >> WHEEL_BITS) & WHEEL_MASK) &&
          cascadeTimers(2, (expireTick_ >> (2 * WHEEL_BITS)) & WHEEL_MASK)) {
        cascadeTimers(3, (expireTick_ >> (3 * WHEEL_BITS)) & WHEEL_MASK);
      }
    }

    auto bi = makeBitIterator(bitmap_.begin());
    *(bi + idx) = false;

    expireTick_++;
    CallbackList* cbs = &buckets_[0][idx];
    while (!cbs->empty()) {
      auto* cb = &cbs->front();
      cbs->pop_front();
      timeoutsToRunNow_.push_back(*cb);
    }
  }

  while (!timeoutsToRunNow_.empty()) {
    auto* cb = &timeoutsToRunNow_.front();
    timeoutsToRunNow_.pop_front();
    count_--;
    cb->wheel_ = nullptr;
    cb->expiration_ = {};
    RequestContextScopeGuard rctx(cb->requestContext_);
    cb->timeoutExpired();
    if (isDestroyed) {
      // The HHWheelTimerBase itself has been destroyed. The other callbacks
      // will have been cancelled from the destructor. Bail before causing
      // damage.
      return;
    }
  }

  // We don't need to schedule a new timeout if there're nothing in the wheel.
  if (count_ > 0) {
    scheduleNextTimeout(expireTick_);
  }
}

template <class Duration>
size_t HHWheelTimerBase<Duration>::cancelAll() {
  size_t count = 0;

  if (count_ != 0) {
    const std::size_t numElements = WHEEL_BUCKETS * WHEEL_SIZE;
    auto maxBuckets = std::min(numElements, count_);
    auto buckets = std::make_unique<CallbackList[]>(maxBuckets);
    size_t countBuckets = 0;
    for (auto& tick : buckets_) {
      for (auto& bucket : tick) {
        if (bucket.empty()) {
          continue;
        }
        count += bucket.size();
        std::swap(bucket, buckets[countBuckets++]);
        if (count >= count_) {
          break;
        }
      }
    }

    for (size_t i = 0; i < countBuckets; ++i) {
      cancelTimeoutsFromList(buckets[i]);
    }
    // Swap the list to prevent potential recursion if cancelAll is called by
    // one of the callbacks.
    CallbackList timeoutsToRunNow;
    timeoutsToRunNow.swap(timeoutsToRunNow_);
    count += cancelTimeoutsFromList(timeoutsToRunNow);
  }

  return count;
}

template <class Duration>
void HHWheelTimerBase<Duration>::scheduleNextTimeout(int64_t nextTick) {
  int64_t tick = 1;

  if (nextTick & WHEEL_MASK) {
    auto bi = makeBitIterator(bitmap_.begin());
    auto bi_end = makeBitIterator(bitmap_.end());
    auto it = folly::findFirstSet(bi + (nextTick & WHEEL_MASK), bi_end);
    if (it == bi_end) {
      tick = WHEEL_SIZE - ((nextTick - 1) & WHEEL_MASK);
    } else {
      tick = std::distance(bi + (nextTick & WHEEL_MASK), it) + 1;
    }
  }

  scheduleNextTimeout(nextTick, tick);
}

template <class Duration>
void HHWheelTimerBase<Duration>::scheduleNextTimeout(
    int64_t nextTick,
    int64_t ticks) {
  scheduleTimeoutInternal(interval_ * ticks);
  expireTick_ = ticks + nextTick - 1;
}

template <class Duration>
size_t HHWheelTimerBase<Duration>::cancelTimeoutsFromList(
    CallbackList& timeouts) {
  size_t count = 0;
  while (!timeouts.empty()) {
    ++count;
    auto& cb = timeouts.front();
    cb.cancelTimeout();
    cb.callbackCanceled();
  }
  return count;
}

template <class Duration>
int64_t HHWheelTimerBase<Duration>::calcNextTick() {
  return calcNextTick(getCurTime());
}

template <class Duration>
int64_t HHWheelTimerBase<Duration>::calcNextTick(
    std::chrono::steady_clock::time_point curTime) {
  return (curTime - startTime_) / interval_;
}

// std::chrono::microseconds
template <>
void HHWheelTimerBase<std::chrono::microseconds>::scheduleTimeoutInternal(
    std::chrono::microseconds timeout) {
  this->AsyncTimeout::scheduleTimeoutHighRes(timeout);
}

// std::chrono::milliseconds
template class HHWheelTimerBase<std::chrono::milliseconds>;

// std::chrono::microseconds
template class HHWheelTimerBase<std::chrono::microseconds>;
} // namespace folly
