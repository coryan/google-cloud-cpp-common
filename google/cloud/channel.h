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
  static auto test(...) -> std::false_type { return {}; }
};

template <typename Sink, typename T>
struct has_shutdown_impl {
  static auto test(int)
      -> sfinae_true<decltype(std::declval<Sink>().shutdown())>;
  static auto test(...) -> std::false_type { return {}; }
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

template <typename Source, typename Sink,
    typename std::enable_if<concepts::is_source<decay_t<Source>>::value,
                            int>::type = 0,
    typename T = concepts::source_event_t<decay_t<Source>>,
    typename std::enable_if<concepts::is_sink<decay_t<Sink>, T>::value,
                            int>::type = 0>
// TODO() - return the error stream
void operator>>(
    Source&& source, Sink&& sink) {
  struct Handler : public std::enable_shared_from_this<Handler> {
    decay_t<Source> source;
    decay_t<Sink> sink;

    Handler(Source&& so, Sink&& si)
    : source(std::move(so)), sink(std::move(si)) {}

    void start() {
      auto self = this->shared_from_this();
      source.on_data([self](T value) {
        self->sink.push(std::move(value));
      });
    }
  };
  auto handler = std::make_shared<Handler>(
      std::forward<Source>(source),
      std::forward<Sink>(sink));
  handler->start();
}

}  // namespace internal

}  // namespace GOOGLE_CLOUD_CPP_NS
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_CHANNEL_H_
