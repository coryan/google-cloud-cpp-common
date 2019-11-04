// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_CHANNEL_H_
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_CHANNEL_H_

#include "google/cloud/future.h"
#include "google/cloud/internal/invoke_result.h"
#include "google/cloud/optional.h"
#include <deque>
#include <limits>

namespace google {
namespace cloud {
inline namespace GOOGLE_CLOUD_CPP_NS {

namespace internal {

enum class queue_state {
  kAccepting,
  kDraining,
  kShutdown,
};

enum class future_contents {
  kHasValue,
  kHasException,
  kDrain,
};

template <typename T>
struct future_state {
  future_contents contents;
  union {
    T value;
    std::exception_ptr exception;
  };
};

template <>
struct future_state<void> {
  future_contents contents;
  std::exception_ptr exception;
};

class action_semaphore {
 public:
  action_semaphore(std::size_t lwm, std::size_t hwm) : lwm_(lwm), hwm_(hwm) {}

  enum handler_resolution {
    kDone,
    kReschedule,
  };
  using notification_handler = std::function<handler_resolution()>;

  void notify() {
    std::unique_lock<std::mutex> lk(mu_);
    if (!pending_.empty()) {
      // Because there is a pending notification_handler, we can just signal it, without
      // any changes to `current_`:
      auto handler = std::move(pending_.front());
      pending_.pop_front();
      lk.unlock();
      invoke_handler(std::move(handler));
      return;
    }
    wait_for_lwm_.wait(lk, [this] { return state_ == state::kReady; });
    ++current_;
    wait_for_not_empty_.notify_all();
    if (current_ >= hwm_) {
      state_ = state::kBlocked;
    }
  }

  void read() {
    std::unique_lock<std::mutex> lk(mu_);
    wait_for_not_empty_.wait(lk, [this] { return current_ > 0; });
    read(lk);
  }

  void on_notify(notification_handler handler) {
    std::unique_lock<std::mutex> lk(mu_);
    if (current_ > 0) {
      read();
      lk.unlock();
      invoke_handler(std::move(handler));
      return;
    }
    pending_.push_back(std::move(handler));
  }

 private:
  void read(std::unique_lock<std::mutex>& ) {
    if (--current_ < lwm_) {
      state_ = state::kReady;
      wait_for_lwm_.notify_all();
    }
  }

  void invoke_handler(notification_handler handler) {
    auto resolution = handler();
    if (resolution == kDone) {
      return;
    }
    on_notify(std::move(handler));
  }

  enum class state {
    kReady,
    kBlocked,
  };

  std::mutex mu_;
  std::condition_variable wait_for_lwm_;
  std::condition_variable wait_for_not_empty_;
  std::size_t lwm_;
  std::size_t hwm_;
  std::size_t current_ = 0;
  state state_ = state::kReady;
  std::deque<notification_handler> pending_;
};

template <typename T>
using future_action = std::function<void(future_state<T>)>;

template <typename T>
class future_queue {
 public:
  future_queue() = default;

  void shutdown() {
    std::unique_lock<std::mutex> lk(mu_);
    if (state_ != queue_state::kAccepting) {
      // Already draining or shutdown.
      return;
    }
    state_ = buffer_.empty() ? queue_state::kShutdown : queue_state::kDraining;
    while (!actions_.empty()) {
      auto action = std::move(actions_.front());
      actions_.pop_front();
      lk.unlock();
      action(future_state<T>{future_contents::kDrain});
      lk.lock();
    }
  }

  future_state<T> pull() {
    std::unique_lock<std::mutex> lk(mu_);
    pull_wait_.wait(lk, [this] { return !buffer_.empty(); });
    auto value = std::move(buffer_.front());
    buffer_.pop_front();
    if (buffer_.size() < lwm_) {
      push_wait_.notify_all();
    }
    lk.unlock();
    return value;
  }

  void push(future_state<T> value) {
    std::unique_lock<std::mutex> lk(mu_);
    if (state_ != queue_state::kAccepting) {
      return;
    }
    if (actions_.empty()) {
      push_wait_.wait(lk, [this] { return buffer_.size() < hwm_; });
      buffer_.push_back(std::move(value));
      pull_wait_.notify_all();
      return;
    }
    auto action = std::move(actions_.front());
    actions_.pop_front();
    lk.unlock();
    action(std::move(value));
  }

  void then(future_action<T> action) {
    std::unique_lock<std::mutex> lk(mu_);
    if (buffer_.empty()) {
      actions_.push_back(std::move(action));
      return;
    }
    auto value = std::move(buffer_.front());
    buffer_.pop_front();
    if (buffer_.size() < lwm_) {
      push_wait_.notify_all();
    }
    lk.unlock();
    action(std::move(value));
  }

 private:
  std::mutex mu_;
  std::condition_variable push_wait_;
  std::condition_variable pull_wait_;
  std::deque<future_state<T>> buffer_;
  std::deque<future_action<T>> actions_;
  std::size_t hwm_ = std::numeric_limits<std::size_t>::max();
  std::size_t lwm_ = std::numeric_limits<std::size_t>::max() - 1;
  queue_state state_ = queue_state::kAccepting;
};

template <typename I>
class sink_impl {
 public:
  virtual ~sink_impl() = default;

  virtual queue_state push(I value) = 0;
  virtual queue_state push_exception(std::exception_ptr) = 0;
  virtual void push_ready(future_action<void> callback) = 0;
  virtual void shutdown() = 0;
};

template <typename O>
class source_impl {
 public:
  virtual ~source_impl() = default;

  virtual optional<O> pull() = 0;
  virtual void pull_ready(future_action<O> callback) = 0;
};

template <typename C>
void connect(std::shared_ptr<source_impl<C>> so,
             std::shared_ptr<sink_impl<C>> si) {
  auto& source = so;
  auto& sink = si;

  auto transfer = [source, sink](future_state<C> source_ready,
                                 future_state<void>) {
    auto state = [&] {
      if (source_ready.contents == future_contents::kHasException) {
        return sink->push_exception(std::move(source_ready.exception));
      }
      return sink->push(std::move(source_ready.value));
    }();
    if (state == queue_state::kShutdown) {
      return;
    }
    connect(source, sink);
  };

  source->then([transfer](future_state<C> source_ready) {
    sink->then([&](future_state<void> sink_ready) {
      transfer(std::move(source_ready), std::move(sink_ready));
    });
  });
}

template <typename T>
class buffered_channel {
 public:
  buffered_channel() = default;

  optional<T> pull() {
    auto s = queue_.pull();
    switch (s.contents) {
      case future_contents::kHasValue:
        return std::move(s.value);
      case future_contents::kHasException:
        std::rethrow_exception(s.exception);
      case future_contents::kDrain:
        return {};
    }
  }

  void push(T value) {
    queue_.push(future_state<T>{future_contents::kHasValue, std::move(value)});
  }

  void shutdown() { queue_.shutdown(); }

  future<queue_state> push_ready() { return {}; }
  future<void> pull_ready() { return {}; }

  class source : public source_impl<T> {
   public:
    source(std::shared_ptr<buffered_channel> channel)
        : channel_(std::move(channel)) {}

    optional<T> pull() override { return channel_->pull(); }
    future<void> pull_ready() override { return channel_->pull_ready(); }

   private:
    std::shared_ptr<buffered_channel> channel_;
  };

  class sink : public sink_impl<T> {
   public:
    sink(std::shared_ptr<buffered_channel> channel)
        : channel_(std::move(channel)) {}

    void push(T value) override { channel_->push(std::move(value)); }
    future<void> push_ready() override { return channel_->push_ready(); }

   private:
    std::shared_ptr<buffered_channel> channel_;
  };

 private:
  future_queue<T> queue_;
  future_queue<void> push_ready_;
};

template <typename T>
std::pair<std::shared_ptr<sink_impl<T>>, std::shared_ptr<source_impl<T>>>
make_buffered_channel_impl() {
  auto channel = std::make_shared<buffered_channel<T>>();
  return {std::make_shared<buffered_channel<T>::sink>(channel),
          std::make_shared<buffered_channel<T>::source>(channel)};
}

}  // namespace internal

template <typename T>
class source;

template <typename T>
class sink;

template <typename T>
class channel {
 public:
  channel() {
    std::tie(source_impl_, sink_impl_) =
        internal::make_buffered_channel_impl<T>();
  }

  std::pair<sink<T>, source<T>> endpoints() &&;

 private:
  std::shared_ptr<internal::source_impl<T>> source_impl_;
  std::shared_ptr<internal::sink_impl<T>> sink_impl_;
};

template <typename T>
class source {
 public:
  source() = default;

  /// Blocks until a value is available, returns that value.
  optional<T> pull() { return impl_->pull(); }

  /// Associates Callable with this generator, called every time a value is
  /// available.
  template <typename Callable,
            typename U = typename internal::invoke_result_t<Callable, T>>
  source<U> on_each(Callable&& /*c*/) && {
    return {};
  }

 private:
  friend class channel<T>;
  explicit source(std::shared_ptr<internal::source_impl<T>> c)
      : impl_(std::move(c)) {}

  std::shared_ptr<internal::source_impl<T>> impl_;
};

template <typename T>
class sink {
 public:
  void push(T value) { impl_->push(std::move(value)); }

 private:
  friend class channel<T>;
  explicit sink(std::shared_ptr<internal::source_impl<T>> impl)
      : impl_(std::move(impl)) {}

  std::shared_ptr<internal::source_impl<T>> impl_;
};

template <typename T>
std::pair<sink<T>, source<T>> channel<T>::endpoints() && {
  return {sink<T>(std::move(sink_impl_)), source<T>(std::move(source_impl_))};
}

}  // namespace GOOGLE_CLOUD_CPP_NS
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_CHANNEL_H_
