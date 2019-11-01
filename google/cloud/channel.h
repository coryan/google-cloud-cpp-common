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

template <typename I, typename O>
class channel_impl {
 public:
  virtual ~channel_impl() = default;

  virtual O pull() = 0;
  virtual void push(I value) = 0;

  virtual future<void> push_ready() = 0;
  virtual future<void> pull_ready() = 0;
};

template <typename I, typename C, typename O>
class connector : public channel_impl<I, O> {
 public:
  connector(std::shared_ptr<channel_impl<I, C>> left,
            std::shared_ptr<channel_impl<C, O>> right);

  void start() {
    auto lr = left_.pull_ready();
    auto rr = right_.push_ready();
    auto ready = when_all(lr, rr);
    auto& left = left_;
    auto& right = right_;
    ready.then([left, right](future<std::tuple<future<void>, future<void>>> f) {
      auto p = f.get();
      std::get<0>(p).get();
      std::get<1>(p).get();
      right->push(left.pull());

    });
  }

 private:
  std::shared_ptr<channel_impl<I, C>> left_;
  std::shared_ptr<channel_impl<C, O>> right_;
};

template <typename T>
class buffered_channel : public channel_impl<T, T> {
 public:
  buffered_channel() = default;

  T next() override {
    std::unique_lock<std::mutex> lk(mu_);
    next_wait_.wait(lk, [this] { return can_pull(); });
    T value = std::move(buffer_.front());
    buffer_.pop_front();
    if (can_push()) {
      push_wait_.notify_all();
    }
    return value;
  }

  void push(T value) override {
    std::unique_lock<std::mutex> lk(mu_);
    next_wait_.wait(lk, [this] { return can_push(); });
    buffer_.push_back(std::move(value));
    next_wait_.notify_all();
  }

  future<void> on_ready() override { return make_ready_future(); }

 private:
  bool can_push() const { return buffer_.size() <= max_size_ - min_capacity_; }
  bool can_pull() const { return !buffer_.empty(); }

  std::mutex mu_;
  std::condition_variable next_wait_;
  std::condition_variable push_wait_;
  std::deque<T> buffer_;
  std::size_t max_size_ = (std::numeric_limits<std::size_t>::max)();
  std::size_t min_capacity_ = 1;
};

template <typename Callable, typename T = internal::invoke_result_t<Callable>>
class generator_channel : public channel_impl<void, T> {
 public:
  explicit generator_channel(Callable&& c) : callable_(std::forward<T>(c)) {}

  T next() override { return callable_(); }
  void push(T) override {}
  future<void> on_ready() { return {}; }

 private:
  Callable callable_;
};

}  // namespace internal

template <typename T>
class generator;

template <typename T>
class sink;

template <typename T>
class channel {
 public:
  channel() : impl_(std::make_shared<internal::buffered_channel<T>>()) {}

  void push(T value) { impl_->push(std::move(value)); }

  future<sink<T>> on_ready() && { return {}; }

  generator<T> generator();

 private:
  friend class sink<T>;
  explicit channel(std::shared_ptr<internal::channel_impl<T>> impl)
      : impl_(std::move(impl)) {}

  std::shared_ptr<internal::channel_impl<T>> impl_;
};

template <typename T>
class generator {
 public:
  generator() = default;

  /// Blocks until a value is available, returns that value.
  T next() { return channel_->next(); }

  /// Associates Callable with this generator, called every time a value is
  /// available.
  template <typename Callable,
            typename U = typename internal::invoke_result_t<Callable, T>>
  generator<U> on_each(Callable&& c) && {
    using channel = internal::composed_channel<T, Callable>;

    return generator<U>(std::make_shared<channel>(std::forward<Callable>(c),
                                                  std::move(channel_)));
  }

 private:
  explicit generator(std::shared_ptr<internal::channel_impl<T>> c)
      : channel_(std::move(c)) {}

  std::shared_ptr<internal::channel_impl<T>> channel_;
};

template <typename T>
class sink {
 public:
  channel<T> operator()(T value) {
    impl_->push(std::move(value));
    return channel<T>(std::move(impl_));
  }

 private:
  friend class channel<T>;
  explicit sink(std::shared_ptr<internal::channel_impl<T>> impl)
      : impl_(std::move(impl)) {}

  std::shared_ptr<internal::channel_impl<T>> impl_;
};

}  // namespace GOOGLE_CLOUD_CPP_NS
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_CHANNEL_H_
