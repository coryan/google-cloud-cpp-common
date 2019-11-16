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

enum class channel_state {
  kAccepting,
  kDraining,
  kShutdown,
};

enum on_data_resolution {
  kDone,
  kReschedule,
};

namespace concepts {
template <typename>
struct sfinae_true : public std::true_type {};

template <typename Sink, typename T>
struct has_push_impl {
  static auto test(int)
      -> sfinae_true<decltype(std::declval<Sink>().push(std::declval<T>()))>;
  static auto test(long) -> std::false_type;
};

template <typename Sink, typename T>
struct has_shutdown_impl {
  static auto test(int)
      -> sfinae_true<decltype(std::declval<Sink>().shutdown())>;
  static auto test(long) -> std::false_type;
};

template <typename Sink, typename T>
struct has_push : decltype(has_push_impl<Sink, T>::test(0)) {};

template <typename Sink, typename T>
struct has_shutdown : decltype(has_push_impl<Sink, T>::test(0)) {};

template <typename Sink, typename T>
struct is_sink : absl::conjunction<has_push<Sink, T>, has_shutdown<Sink, T>> {};

template <typename Source, typename T>
struct has_pull_impl {
  static auto test(int) -> sfinae_true<decltype(std::declval<Source>().pull())>;
  static auto test(long) -> std::false_type;
};
template <typename Source, typename T>
struct has_pull : decltype(has_pull_impl<Source, T>::test(0)) {};

template <typename Source, typename T>
struct has_on_data_impl {
  struct handler {
    on_data_resolution operator()(T) { return on_data_resolution::kDone; }
  };
  static auto test(int)
      -> sfinae_true<decltype(std::declval<Source>().on_data(handler{}))>;
  static auto test(long) -> std::false_type;
};
template <typename Source, typename T>
struct has_on_data : decltype(has_on_data_impl<Source, T>::test(0)) {};

template <typename Source, class U = void>
struct has_event_type : public std::false_type {};

template <typename Source>
struct has_event_type<Source, void_t<typename Source::event_type>>
    : public std::true_type {};

struct do_not_use {};

template <typename Source, class U = void>
struct source_event_type {
  using type = do_not_use;
};

template <typename Source>
struct source_event_type<Source, void_t<typename Source::event_type>> {
  using type = typename Source::event_type;
};

template <typename Source>
using source_event_t = typename source_event_type<Source>::type;

template <typename Source, class U = void>
struct pull_result_type {
  using type = do_not_use;
};
template <typename Source>
struct pull_result_type<Source, void_t<decltype(&Source::pull)>> {
  using type = invoke_result_t<decltype(&Source::pull), Source>;
};

template <typename Source>
using source_pull_result_t = typename pull_result_type<Source>::type;

template <typename Source>
struct is_source
    : absl::conjunction<has_event_type<Source>,
                        has_pull<Source, source_event_t<Source>>,
                        std::is_same<optional<source_event_t<Source>>,
                                     source_pull_result_t<Source>>,
                        has_on_data<Source, source_event_t<Source>>> {};

}  // namespace concepts

/**
 * A thread-safe queue with high and low watermarks to control flow.
 */
template <typename T>
class simple_channel {
 public:
  using on_data_handler = std::function<on_data_resolution(T)>;

  simple_channel() = default;
  simple_channel(std::size_t lwm, std::size_t hwm) : lwm_(lwm), hwm_(hwm) {}

  void shutdown() {
    std::unique_lock<std::mutex> lk(mu_);
    if (state_ != channel_state::kAccepting) {
      // Already draining or shutdown.
      return;
    }
    state_ =
        buffer_.empty() ? channel_state::kShutdown : channel_state::kDraining;
    // Invoke any pending callbacks.
    while (!on_has_data_.empty()) {
      auto action = std::move(on_has_data_.front());
      on_has_data_.pop_front();
      lk.unlock();
      action(T{});
      lk.lock();
    }
  }

  optional<T> pull() {
    std::unique_lock<std::mutex> lk(mu_);
    pull_wait_.wait(lk, [this] {
      return !buffer_.empty() || state_ == channel_state::kShutdown;
    });
    auto value = pop(lk);
    lk.unlock();
    return value;
  }

  void push(T value) {
    std::unique_lock<std::mutex> lk(mu_);
    if (state_ != channel_state::kAccepting) {
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

  void on_has_data(on_data_handler h) {
    std::unique_lock<std::mutex> lk(mu_);
    if (!buffer_.empty()) {
      auto value = pop(lk);
      lk.unlock();
      if (value.has_value()) return;
      invoke_has_data_handler(std::move(h), *std::move(value));
      return;
    }
    on_has_data_.push_back(std::move(h));
  }

  class source {
   public:
    using event_type = T;
    explicit source(std::shared_ptr<simple_channel> channel)
        : channel_(std::move(channel)) {}

    optional<T> pull() { return channel_->pull(); }

    void on_data(on_data_handler handler) {
      channel_->on_has_data(std::move(handler));
    }

   private:
    std::shared_ptr<simple_channel> channel_;
  };

  class sink {
   public:
    explicit sink(std::shared_ptr<simple_channel> channel)
        : channel_(std::move(channel)) {}

    void push(T value) { channel_->push(std::move(value)); }
    void shutdown() { channel_->shutdown(); }

   private:
    std::shared_ptr<simple_channel> channel_;
  };

  struct endpoints {
    sink tx;
    source rx;
  };

 private:
  optional<T> pop(std::unique_lock<std::mutex>&) {
    if (state_ == channel_state::kShutdown) {
      return {};
    }
    auto value = std::move(buffer_.front());
    buffer_.pop_front();
    if (state_ == channel_state::kDraining && buffer_.size() < lwm_) {
      state_ = channel_state::kAccepting;
      push_wait_.notify_all();
    }
    return value;
  }

  void invoke_has_data_handler(on_data_handler handler, T value) {
    auto resolution = handler(std::move(value));
    if (resolution == kDone) {
      return;
    }
    on_has_data(std::move(handler));
  }

  // TODO(...) - this might benefit from using a lock-free queue in some cases.
  std::mutex mu_;
  std::condition_variable push_wait_;
  std::condition_variable pull_wait_;
  std::deque<T> buffer_;
  std::deque<on_data_handler> on_has_data_;
  std::size_t lwm_ = std::numeric_limits<std::size_t>::max() - 1;
  std::size_t hwm_ = std::numeric_limits<std::size_t>::max();
  channel_state state_ = channel_state::kAccepting;
};

template <typename T>
typename simple_channel<T>::endpoints make_simple_channel_impl() {
  auto channel = std::make_shared<simple_channel<T>>();
  return {typename simple_channel<T>::sink(channel),
          typename simple_channel<T>::source(channel)};
}

template <typename T>
typename simple_channel<T>::endpoints make_simple_channel_impl(
    std::size_t lwm, std::size_t hwm) {
  auto channel = std::make_shared<simple_channel<T>>(lwm, hwm);
  return {typename simple_channel<T>::sink(channel),
          typename simple_channel<T>::source(channel)};
}

template <typename T, typename Source>
class flow_control_junction {
 public:
  using capacity_channel = simple_channel<bool>;
  using capacity_sink = typename simple_channel<bool>::sink;
  using capacity_source = typename simple_channel<bool>::source;

  flow_control_junction(Source source, capacity_sink capacity)
      : source_(std::move(source)), capacity_(std::move(capacity)) {
    static_assert(
        concepts::is_source<Source>::value,
        "The Source template parameter should meet is_source<Source, T>");
    static_assert(concepts::is_source<flow_control_junction>::value,
                  "flow_control_junction should meet "
                  "is_source<flow_control_junction, T>");
  }

  optional<T> pull() {
    auto value = source_.pull();
    capacity_.push(true);
    return std::move(value);
  }

  template <typename Handler>
  void on_data(Handler handler) {
    struct Wrapper {
      flow_control_junction* self;
      typename std::decay<Handler>::type handler;

      on_data_resolution operator()(T value) {
        auto r = handler(std::move(value));
        self->capacity_.push(true);
        return r;
      }
    };
    source_.on_data(Wrapper{this, std::forward<Handler>(handler)});
  }

 private:
  Source source_;
  capacity_sink capacity_;
};

template <typename T, typename Source, typename Sink>
struct flow_control_endpoints {
  Sink tx;
  flow_control_junction<T, Source> rx;
  typename flow_control_junction<T, Source>::capacity_source capacity;
};

template <typename T, typename Source, typename Sink>
flow_control_endpoints<T, Source, Sink> flow_control_impl(
    Source source, Sink sink, std::size_t capacity) {
  // TODO(coryan) - use a semaphore-like object ...
  auto cap_endpoints = make_simple_channel_impl<bool>(capacity, 2 * capacity);

  flow_control_junction<T, Source> junction(std::move(source),
                                            std::move(cap_endpoints.tx));
  return {std::move(sink), std::move(junction), std::move(cap_endpoints.rx)};
}

template <typename Callable, typename T = invoke_result_t<Callable>>
class generator_source {
 public:
  using event_type = T;

  explicit generator_source(Callable&& callable)
      : callable_(std::move(callable)) {}

  optional<T> pull() { return callable_(); }

  template <typename Handler>
  void on_data(Handler handler) {
    for (auto r = handler(callable_()); r == on_data_resolution::kReschedule;
         r = handler(callable_())) {
    }
  }

 private:
  Callable callable_;
};

template <typename T>
using decay_t = typename std::decay<T>::type;

template <typename Callable>
generator_source<decay_t<Callable>> generator(Callable&& callable) {
  static_assert(
      concepts::is_source<generator_source<decay_t<Callable>>>::value,
      "generator_source<> should meet concepts::is_source<> requirements");
  return generator_source<decay_t<Callable>>(std::forward<Callable>(callable));
}

template <typename Source, typename Pipeline>
class transformed_source {
 public:
  using event_type = invoke_result_t<Pipeline, typename Source::event_type>;
  transformed_source(Source source, Pipeline pipeline)
      : source_(std::move(source)), pipeline_(std::move(pipeline)) {}

  optional<event_type> pull() {
    auto tmp = source_.pull();
    if (!tmp) return {};
    return pipeline_(*std::move(tmp));
  }

  template <typename Handler>
  void on_data(Handler&& handler) {
    struct Wrapper {
      transformed_source* self;
      decay_t<Handler> handler;

      on_data_resolution operator()(typename Source::event_type x) {
        return handler(self->pipeline_(std::move(x)));
      }
    };
    source_.on_data(Wrapper{this, std::forward<Handler>(handler)});
  }

 private:
  Source source_;
  Pipeline pipeline_;
};

template <typename Source, typename Transform,
          typename std::enable_if<concepts::is_source<decay_t<Source>>::value,
                                  int>::type = 0,
          typename T = concepts::source_event_t<decay_t<Source>>,
          typename U = invoke_result_t<decay_t<Transform>, T>>
transformed_source<decay_t<Source>, decay_t<Transform>> operator>>(
    Source&& source, Transform&& transform) {
  return transformed_source<decay_t<Source>, decay_t<Transform>>(
      std::forward<Source>(source), std::forward<Transform>(transform));
}

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

template <typename T>
using on_data_handler = std::function<on_data_resolution(future_state<T>)>;

using on_capacity_handler = std::function<void(channel_state)>;

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
    if (state_ != channel_state::kAccepting) {
      // Already draining or shutdown.
      return;
    }
    state_ =
        buffer_.empty() ? channel_state::kShutdown : channel_state::kDraining;
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
    if (state_ != channel_state::kAccepting) {
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
    if (state_ == channel_state::kAccepting) {
      lk.unlock();
      invoke_has_capacity_handler(h, channel_state::kAccepting);
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
    if (state_ == channel_state::kDraining && buffer_.size() < lwm_) {
      state_ = channel_state::kAccepting;
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
                                   channel_state state) {
    handler(state);
  }

  std::mutex mu_;
  std::condition_variable push_wait_;
  std::condition_variable pull_wait_;
  std::deque<future_state<T>> buffer_;
  std::size_t hwm_ = std::numeric_limits<std::size_t>::max();
  std::size_t lwm_ = std::numeric_limits<std::size_t>::max() - 1;
  channel_state state_ = channel_state::kAccepting;
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
    if (state == channel_state::kShutdown) {
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
#endif  // 0

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
