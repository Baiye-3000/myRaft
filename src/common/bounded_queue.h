#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <utility>

namespace distributed_kv::common {

/**
 * Fixed-capacity multi-producer/multi-consumer queue with shutdown support.
 */
template <typename T>
class BoundedQueue final {
 public:
  explicit BoundedQueue(std::size_t capacity) : capacity_(capacity) {}

  BoundedQueue(const BoundedQueue&) = delete;
  BoundedQueue& operator=(const BoundedQueue&) = delete;

  /**
   * Enqueues without blocking.
   *
   * Input: movable value. Output: false when full or closed.
   * Thread safety: safe for concurrent producers and consumers.
   */
  [[nodiscard]] bool tryPush(T value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_ || capacity_ == 0 || queue_.size() >= capacity_) {
      return false;
    }
    queue_.push_back(std::move(value));
    available_.notify_one();
    return true;
  }

  /**
   * Waits up to timeout for one value.
   *
   * Input: writable value and duration. Output: false on timeout/closed-empty.
   * Thread safety: safe for concurrent producers and consumers.
   */
  template <typename Rep, typename Period>
  [[nodiscard]] bool popFor(
      T& value, const std::chrono::duration<Rep, Period>& timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!available_.wait_for(lock, timeout,
                             [this] { return closed_ || !queue_.empty(); }) ||
        queue_.empty()) {
      return false;
    }
    value = std::move(queue_.front());
    queue_.pop_front();
    return true;
  }

  /**
   * Wakes waiters and permanently rejects new values.
   *
   * Input/output: none. Thread safety: idempotent and concurrent-safe.
   */
  void close() {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
    available_.notify_all();
  }

  [[nodiscard]] std::size_t size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
  }

 private:
  const std::size_t capacity_;
  mutable std::mutex mutex_;
  std::condition_variable available_;
  std::deque<T> queue_;
  bool closed_{false};
};

}  // namespace distributed_kv::common
