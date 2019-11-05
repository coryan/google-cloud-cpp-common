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
#include <absl/types/variant.h>
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

template <typename T>
struct future_state : absl::variant<absl::monostate, std::exception_ptr, T> {
  using absl::variant<absl::monostate, std::exception_ptr, T>::variant;
};

template <>
struct future_state<void> : absl::variant<absl::monostate, std::exception_ptr> {
  using absl::variant<absl::monostate, std::exception_ptr>::variant;
};

template <typename T>
using future_action = std::function<void(future_state<T>)>;

enum on_data_resolution {
  kDone,
  kReschedule,
};

template <typename T>
using on_data_handler = std::function<on_data_resolution(future_state<T>)>;

using on_capacity_handler = std::function<void(queue_state)>;

template <typename I>
class sink_impl {
 public:
  virtual ~sink_impl() = default;

  virtual void push(I value) = 0;
  virtual void push_exception(std::exception_ptr) = 0;
  virtual void on_has_capacity(on_capacity_handler h) = 0;
  virtual void shutdown() = 0;
};

template <typename O>
class source_impl {
 public:
  virtual ~source_impl() = default;

  virtual optional<O> pull() = 0;
  virtual void on_has_data(on_data_handler<O> h) = 0;
};

// A queue with lwm and hwm for capacity.
template <typename T>
class buffered_channel {
 public:
  buffered_channel() = default;

  void shutdown() {
    std::unique_lock<std::mutex> lk(mu_);
    if (state_ != queue_state::kAccepting) {
      // Already draining or shutdown.
      return;
    }
    state_ = buffer_.empty() ? queue_state::kShutdown : queue_state::kDraining;
    while (!on_has_data_.empty()) {
      auto action = std::move(on_has_data_.front());
      on_has_data_.pop_front();
      lk.unlock();
      action(future_state<T>{});
      lk.lock();
    }
  }

  future_state<T> pull() {
    std::unique_lock<std::mutex> lk(mu_);
    pull_wait_.wait(lk, [this] { return !buffer_.empty(); });
    auto value = pop(lk);
    lk.unlock();
    return value;
  }

  void push(future_state<T> value) {
    std::unique_lock<std::mutex> lk(mu_);
    if (state_ != queue_state::kAccepting) {
      return;
    }
    if (on_has_data_.empty()) {
      push_wait_.wait(lk, [this] { return buffer_.size() < hwm_; });
      buffer_.push_back(std::move(value));
      pull_wait_.notify_all();
      return;
    }
    auto action = std::move(on_has_data_.front());
    on_has_data_.pop_front();
    lk.unlock();
    invoke_has_data_handler(std::move(action), std::move(value));
  }

  void on_has_data(on_data_handler<T> h) {
    std::unique_lock<std::mutex> lk(mu_);
    if (!buffer_.empty()) {
      auto value = pop(lk);
      lk.unlock();
      invoke_has_data_handler(std::move(h), std::move(value));
      return;
    }
    on_has_data_.push_back(std::move(h));
  }

  void on_has_capacity(on_capacity_handler h) {
    std::unique_lock<std::mutex> lk(mu_);
    if (state_ == queue_state::kAccepting) {
      lk.unlock();
      invoke_has_capacity_handler(h, queue_state::kAccepting);
      return;
    }
    on_has_capacity_.push_back(std::move(h));
  }

  class source : public source_impl<T> {
   public:
    explicit source(std::shared_ptr<buffered_channel> channel)
        : channel_(std::move(channel)) {}

    optional<T> pull() override {
      auto value = channel_->pull();
      switch (value.index()) {
        case 0:
          return {};
        case 1:
          std::rethrow_exception(absl::get<std::exception_ptr>(value));
        case 2:
          return absl::get<T>(value);
        default:
          break;
      }
      throw std::future_error(std::future_errc::no_state);
    }

    void on_has_data(on_data_handler<T> handler) override {
      channel_->on_has_data(std::move(handler));
    }

   private:
    std::shared_ptr<buffered_channel> channel_;
  };

  class sink : public sink_impl<T> {
   public:
    explicit sink(std::shared_ptr<buffered_channel> channel)
        : channel_(std::move(channel)) {}

    void push(T value) override { channel_->push(std::move(value)); }
    void push_exception(std::exception_ptr ex) override {
      channel_->push(std::move(ex));
    }
    void on_has_capacity(on_capacity_handler h) override {
      channel_->on_has_capacity(std::move(h));
    }
    void shutdown() override { channel_->shutdown(); }

   private:
    std::shared_ptr<buffered_channel> channel_;
  };

 private:
  future_state<T> pop(std::unique_lock<std::mutex>&) {
    auto value = std::move(buffer_.front());
    buffer_.pop_front();
    if (state_ == queue_state::kDraining && buffer_.size() < lwm_) {
      state_ = queue_state::kAccepting;
      push_wait_.notify_all();
    }
    return value;
  }

  void invoke_has_data_handler(on_data_handler<T> handler,
                               future_state<T> value) {
    auto resolution = handler(std::move(value));
    if (resolution == kDone) {
      return;
    }
    on_has_data(std::move(handler));
  }

  void invoke_has_capacity_handler(on_capacity_handler const& handler,
                                   queue_state state) {
    handler(state);
  }

  std::mutex mu_;
  std::condition_variable push_wait_;
  std::condition_variable pull_wait_;
  std::deque<future_state<T>> buffer_;
  std::size_t hwm_ = std::numeric_limits<std::size_t>::max();
  std::size_t lwm_ = std::numeric_limits<std::size_t>::max() - 1;
  queue_state state_ = queue_state::kAccepting;
  std::deque<on_data_handler<T>> on_has_data_;
  std::deque<on_capacity_handler> on_has_capacity_;
};

template <typename T>
std::pair<std::shared_ptr<sink_impl<T>>, std::shared_ptr<source_impl<T>>>
make_buffered_channel_impl() {
  auto channel = std::make_shared<buffered_channel<T>>();
  return {std::make_shared<typename buffered_channel<T>::sink>(channel),
          std::make_shared<typename buffered_channel<T>::source>(channel)};
}

#if 0
template <typename C>
void connect(std::shared_ptr<source_impl<C>> so,
             std::shared_ptr<sink_impl<C>> si) {
  auto& source = so;
  auto& sink = si;

  auto transfer = [source, sink](future_state<C> source_ready,
                                 future_state<void>) {
    auto state = [&] {
      if (source_ready.index() == 1U) {
        return sink->push_exception(std::move(
            absl::get<std::exception_ptr>(source_ready)));
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
#endif // 0

}  // namespace internal

#if 0
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
#endif  // 0

}  // namespace GOOGLE_CLOUD_CPP_NS
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_CHANNEL_H_
