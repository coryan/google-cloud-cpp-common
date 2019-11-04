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

template <typename T>
using decay_t = typename std::decay<T>::type;

template <typename... Futures>
auto when_all(Futures&&... /*futures*/)
    -> future<std::tuple<decay_t<Futures>...>>;

namespace internal {

enum class SinkState {
  kReady,
  kShutdown,
};

enum class SourceState {
  kReady,
  kShutdown,
};

enum class future_contents {
  kHasValue,
  kHasException,
};

template <typename T>
struct future_state {
  future_contents contents;
  union {
    T value;
    std::exception_ptr exception;
  };
};

template<>
struct future_state<void> {
  future_contents contents;
  std::exception_ptr exception;
};

template <typename T>
using future_action = std::function<void(future_state<T>)>;

template <typename T>
class future_queue {
 public:
  future_state<T> pull() {
    std::unique_lock<std::mutex> lk(mu_);
    pull_wait_.wait(lk, [this] {
      return !buffer_.empty();
    });
    auto value = std::move(buffer_.front());
    buffer_.pop_front();
    lk.unlock();
    return value;
  }

  void push(future_state<T> value) {
    std::unique_lock<std::mutex> lk(mu_);
    if (actions_.empty()) {
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
    lk.unlock();
    action(std::move(value));
  }

 private:
  std::mutex mu_;
  std::condition_variable pull_wait_;
  std::deque<future_state<T>> buffer_;
  std::deque<future_action<T>> actions_;
};

template <typename I>
class sink_impl {
 public:
  virtual ~sink_impl() = default;

  virtual SinkState push(I value) = 0;
  virtual future<SinkState> push_ready() = 0;
  virtual void shutdown() = 0;
};

template <typename O>
class source_impl {
 public:
  virtual ~source_impl() = default;

  virtual optional<O> pull() = 0;
  virtual future<void> pull_ready() = 0;
};

template <typename C>
void connect(std::shared_ptr<source_impl<C>> so,
             std::shared_ptr<sink_impl<C>> si) {
  auto& source = so;
  auto& sink = si;
  auto ready = when_all(source->pull_ready(), sink->push_ready());
  ready.then([source, sink](decltype(ready) f) mutable {
    auto p = f.get();
    auto rx = std::get<0>(p).get();
    auto tx = std::get<1>(p).get();
    auto value = source->pull();
    if (!value.has_value()) {
      sink->shutdown();
      return;
    }
    sink->push(*std::move(value));
    connect(std::move(source), std::move(sink));
  });
}

template <typename T>
class buffered_channel {
 public:
  buffered_channel() = default;

  optional<T> pull() {
    std::unique_lock<std::mutex> lk(mu_);
    if (is_shutdown_) {
      return {};
    }
    next_wait_.wait(lk, [this] { return can_pull(); });
    T value = std::move(buffer_.front());
    buffer_.pop_front();
    if (can_push()) {
      push_wait_.notify_all();
    }
    return value;
  }

  void push(T value) {
    std::unique_lock<std::mutex> lk(mu_);
    next_wait_.wait(lk, [this] { return can_push(); });
    buffer_.push_back(std::move(value));
    next_wait_.notify_all();
  }

  void shutdown() {
    std::lock_guard<std::mutex> lk(mu_);
    is_shutdown_ = true;
  }

  future<SinkState> push_ready() { return {}; }
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
  bool can_push() const { return buffer_.size() <= max_size_ - min_capacity_; }
  bool can_pull() const { return !buffer_.empty(); }

  std::mutex mu_;
  std::condition_variable next_wait_;
  std::condition_variable push_wait_;
  std::deque<T> buffer_;
  bool is_shutdown_ = false;
  std::size_t max_size_ = (std::numeric_limits<std::size_t>::max)();
  std::size_t min_capacity_ = 1;
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
    std::tie(source_impl_, sink_impl_) = internal::make_buffered_channel_impl<T>();
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
  source<U> on_each(Callable&& /*c*/) && { return {}; }

 private:
  friend class channel<T>;
  explicit source(std::shared_ptr<internal::source_impl<T>> c)
      : impl_(std::move(c)) {}

  std::shared_ptr<internal::source_impl<T>> impl_;
};

template <typename T>
class sink {
 public:
  void push(T value) {
    impl_->push(std::move(value));
  }

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
