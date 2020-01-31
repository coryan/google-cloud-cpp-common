// Copyright 2019 Google LLC
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

#include "google/cloud/future.h"
#include "google/cloud/grpc_utils/completion_queue.h"
#include "google/cloud/optional.h"
#include "google/cloud/testing_util/assert_ok.h"
#include "google/cloud/testing_util/chrono_literals.h"
#include <gmock/gmock.h>
#include <deque>

namespace google {
namespace cloud {
inline namespace GOOGLE_CLOUD_CPP_NS {
namespace internal {
namespace {

using ::testing::ElementsAre;

template <typename T>
class satisfied_future;

/**
 * Meets the interface of future<void> without the allocation and locking
 * overhead.
 */
template <>
class satisfied_future<void> {
 public:
  explicit satisfied_future() = default;

  satisfied_future(satisfied_future&&) = default;
  satisfied_future(satisfied_future const&) = default;
  satisfied_future& operator=(satisfied_future&&) = default;
  satisfied_future& operator=(satisfied_future const&) = default;

  void get() {}

  // TODO(coryan) - unwrap functions returning future<U>
  // TODO(coryan) - unwrap functions returning satisfied_future<U>
  // TODO(coryan) - use SFINAE to overload for functions taking void or
  //   future<T> instead of future<T>
  template <typename Callable,
            typename U = invoke_result_t<Callable, satisfied_future<void>>>
  satisfied_future<U> then(Callable&& callable) {
    return satisfied_future<U>(callable(*this));
  }
};

/**
 * Meets the interface of future<T> without the allocation and locking overhead.
 */
template <typename T>
class satisfied_future {
 public:
  explicit satisfied_future(T value) : value_(std::move(value)) {}

  satisfied_future(satisfied_future&&) = default;
  satisfied_future(satisfied_future const&) = default;
  satisfied_future& operator=(satisfied_future&&) = default;
  satisfied_future& operator=(satisfied_future const&) = default;

  T get() { return std::move(value_); }

  // TODO(coryan) - unwrap functions returning future<U>
  // TODO(coryan) - unwrap functions returning satisfied_future<U>
  // TODO(coryan) - use SFINAE to overload for functions taking T or future<T>
  //   instead of future<T>
  template <typename Callable,
            typename U = invoke_result_t<Callable, satisfied_future<T>>>
  satisfied_future<U> then(Callable&& callable) {
    return satisfied_future<U>(callable(*this));
  }

 private:
  T value_;
};

template <typename T>
class simple_source {
 public:
  virtual void start() {}
  virtual future<optional<T>> async_pull_one() { return {}; }
};

template <typename T>
class simple_sink {
 public:
  virtual void shutdown() {}
  virtual future<void> async_push_one(T) { return {}; }
};

template <typename I, typename O>
struct channel {
  simple_sink<I> tx;
  simple_source<O> rx;
};

struct pipeline {
  virtual void start() {}
};

template <typename T>
channel<T, T> keep_in_flight(int) {
  return {};
}

template <typename T, typename Functor>
simple_source<internal::invoke_result_t<Functor, T>> operator|(simple_source<T>,
                                                               Functor&&) {
  return {};
}

template <typename T>
pipeline operator|(simple_source<T>, simple_sink<T>) {
  return {};
}

template <typename T>
class bound_source : public simple_source<T> {
 public:
  bound_source(pipeline p, simple_source<T> s)
      : pipeline_(std::move(p)), source_(std::move(s)) {}

  void start() override { pipeline_.start(); }

 private:
  pipeline pipeline_;
  simple_source<T> source_;
};

template <typename T>
simple_source<T> operator|(simple_source<T> s, channel<T, T> c) {
  auto p = s | std::move(c.tx);
  return bound_source<T>(std::move(p), std::move(c.rx));
}

template <typename I, typename O>
class transform_channel_state {
 public:
  using Generator = std::function<void(I, simple_sink<O>&)>;
  explicit transform_channel_state(Generator gen) {
    auto in = channel<I, I>{};
    channel_input_ = std::move(in.tx);
    generator_input_ = std::move(in.rx);

    generator_ = std::move(gen);

    auto out = channel<O, O>{};
    generator_output_ = std::move(out.tx);
    channel_output_ = std::move(out.rx);
  }

  //@{
  void start() {
    schedule();
    generator_input_.start();
  }
  future<optional<O>> async_pull_one() {
    return channel_output_.async_pull_one();
  }
  //@}

  //@{
  void shutdown() { channel_input_.shutdown(); }
  future<void> async_push_one(I value) {
    return channel_input_.async_push_one(std::move(value));
  }
  //@}

 private:
  void schedule() {
    generator_input_.async_pull_one().then([this](future<optional<I>> f) {
      auto o = f.get();
      if (!o.has_value()) {
        generator_output_.shutdown();
        return;
      }
      generator_(*std::move(o), generator_output_);
      schedule();
    });
  }

  simple_sink<I> channel_input_;
  simple_source<I> generator_input_;
  Generator generator_;
  simple_sink<I> generator_output_;
  simple_source<O> channel_output_;
};

template <typename T, typename Channel>
class channel_source : simple_source<T> {
 public:
  explicit channel_source(std::shared_ptr<Channel> c)
      : channel_(std::move(c)) {}

  future<optional<T>> async_pull_one() override {
    return channel_->async_pull_one();
  }
  void start() override { return channel_->start(); }

 private:
  std::shared_ptr<Channel> channel_;
};

template <typename T, typename Channel>
class channel_sink : simple_sink<T> {
 public:
  explicit channel_sink(std::shared_ptr<Channel> c) : channel_(std::move(c)) {}

  future<void> async_push_one(T value) override {
    return channel_->async_push_one(std::move(value));
  }
  void shutdown() override { channel_->shutdown(); }

 private:
  std::shared_ptr<Channel> channel_;
};

template <typename I, typename O, typename Generator>
channel<I, O> transform_channel(Generator gen) {
  using Channel = transform_channel_state<I, O>;
  auto c = std::make_shared<Channel>(std::move(gen));
  channel_sink<I, Channel> sink(c);
  channel_source<O, Channel> source(c);
  // TODO(coryan) - fix the slicing.
  return {};
}

void by_line(std::string const& input, simple_sink<std::string> output) {
  std::istringstream is(input);
  std::string line;
  while (std::getline(is, line)) {
    output.async_push_one(std::move(line)).get();
  }
}

#if 0
template <typename T>
class in_flight_connector {
 public:
  in_flight_connector(int n) : n_(n) {}

  void start() {
    for (int i = 0; i != n_; ++i) done();
  }

  void send_one(optional<std::string> v) {
    if (!v.has_value()) shutdown();
    output_.async_push_one(*std::move(v)).then([this](future<void>) {
      done();
    });
  }

  void shutdown() { output_.shutdown(); }

 private:
  void done() {
    input_.async_pull_one().then(
        [this](future<optional<std::string>> f) { send_one(f.get()); });
  }

  int n_;
  simple_source<T> input_;
  simple_sink<T> output_;
};
#endif  //

class PeriodicIota : public std::enable_shared_from_this<PeriodicIota> {
 public:
  // TODO(coryan) - Maybe we do not need to require this
  using value_type = int;

  template <typename Duration>
  static std::shared_ptr<PeriodicIota> Create(grpc_utils::CompletionQueue cq,
                                              int count,
                                              Duration const& period) {
    return std::shared_ptr<PeriodicIota>(
        new PeriodicIota(std::move(cq), count, period));
  }

  future<void> start() {
    std::unique_lock<std::mutex> lk(mu_);
    Reschedule(lk);
    return done_.get_future();
  }

  future<optional<int>> async_pull_one() {
    std::lock_guard<std::mutex> lk(mu_);
    if (counter_ >= counter_limit_) {
      return make_ready_future(optional<int>{});
    }
    if (ready_.empty()) {
      waiters_.push_back(promise<optional<int>>{});
      return waiters_.back().get_future();
    }
    auto p = std::move(ready_.front());
    ready_.pop_front();
    return p.get_future();
  }

 private:
  template <typename Duration>
  PeriodicIota(grpc_utils::CompletionQueue cq, int count,
               Duration const& period)
      : cq_(std::move(cq)),
        counter_limit_(count),
        period_(std::chrono::duration_cast<std::chrono::microseconds>(period)) {
  }

  void OnTimer() {
    std::unique_lock<std::mutex> lk(mu_);
    if (waiters_.empty()) {
      promise<optional<int>> p;
      p.set_value(counter_);
      ++counter_;
      ready_.push_back(std::move(p));
      Reschedule(lk);
      return;
    }
    waiters_.front().set_value(counter_);
    waiters_.pop_front();
    ++counter_;
    Reschedule(lk);
  }

  void Reschedule(std::unique_lock<std::mutex>& lk) {
    if (counter_ >= counter_limit_) {
      done_.set_value();
      return;
    }
    lk.unlock();

    auto self = shared_from_this();
    cq_.MakeRelativeTimer(period_).then(
        [self](future<StatusOr<std::chrono::system_clock::time_point>> f) {
          auto tp = f.get();
          ASSERT_STATUS_OK(tp);  // No expected in tests.
          self->OnTimer();
        });
  }

  grpc_utils::CompletionQueue cq_;
  std::mutex mu_;
  int counter_ = 0;
  int counter_limit_;
  std::chrono::microseconds period_;
  std::deque<promise<optional<int>>> waiters_;
  std::deque<promise<optional<int>>> ready_;

  promise<void> done_;
};

struct sfinae_false : public std::false_type {};
struct sfinae_true : public std::true_type {};
struct do_not_use {};

template <typename Source, typename Functor, typename AlwaysVoid = void>
struct has_on_each_impl {
  using type = std::false_type;
};

template <typename Source, typename Functor>
struct has_on_each_impl<
    Source, Functor,
    void_t<decltype(std::declval<Source>().on_each(std::declval<Functor>()))>> {
  using type = std::true_type;
};

template <typename Source, typename Functor>
struct has_on_each : has_on_each_impl<Source, Functor>::type {};

template <typename Source, typename Transform, typename T, typename U>
class transformed_source_async {
 public:
  // TODO(coryan) - Maybe we do not need to require this
  using value_type = U;

  transformed_source_async(Source source, Transform transform)
      : source_(std::move(source)), transform_(std::move(transform)) {}

  auto start() -> decltype(std::declval<Source>().start()) {
    return source_.start();
  }

  future<optional<U>> async_pull_one() {
    // TODO(coryan) - think about lifetime implications of capturing `this`.
    source_.async_pull_one().then([this](future<optional<T>> f) {
      auto value = f.get();
      if (!value) return make_ready_future<optional<U>>({});
      return make_ready_future(transform_(*std::move(value)));
    });
  }

 private:
  Source source_;
  Transform transform_;
};

template <typename T>
using decay_t = typename std::decay<T>::type;

/**
 * Transform a source where there is no "on_each" member function.
 */
template <typename Source, typename Transform,
          typename T = typename Source::value_type,
          typename U = invoke_result_t<Transform, T>,
          typename std::enable_if<!has_on_each<Source, Transform>::value,
                                  int>::type = 0>
transformed_source_async<decay_t<Source>, Transform, T, U> transform_source(
    Source&& source, Transform&& transform) {
  return transformed_source_async<decay_t<Source>, decay_t<Transform>, T, U>(
      std::forward<Source>(source), std::forward<Transform>(transform));
}

/**
 * Transform a source where there is no "on_each" member function.
 */
template <typename Source, typename Transform,
    typename T = typename Source::value_type,
    typename U = invoke_result_t<Transform, T>,
    typename std::enable_if<has_on_each<Source, Transform>::value,
                            int>::type = 0>

transformed_source_async<decay_t<Source>, Transform, T, U> transform_source(
    Source&& source, Transform&& transform) {
  return source.on_each(std::forward<Transform>(transform));
}

TEST(ChannelTest, Goal) {
  pipeline p = simple_source<int>{} | [](int x) { return std::to_string(x); } |
               transform_channel<std::string, std::string>(by_line) |
               keep_in_flight<std::string>(8) | simple_sink<std::string>{};
  p.start();
}

TEST(ChannelTest, MinimalRequirements) {
  // Create a generator that uses a completion queue with a thread pool and
  // timers to generate events.
  grpc_utils::CompletionQueue cq;
  std::vector<std::thread> pool(4);
  std::generate_n(pool.begin(), pool.size(), [&cq] {
    return std::thread{[](grpc_utils::CompletionQueue q) { q.Run(); }, cq};
  });

  using us = std::chrono::microseconds;
  auto generator = PeriodicIota::Create(cq, 5, us(10));

  generator->start();
  std::atomic<bool> done(false);
  std::vector<int> results;
  while (!done.load()) {
    auto next = generator->async_pull_one();
    next.then([&done, &results](future<optional<int>> f) {
      auto v = f.get();
      if (!v) {
        done.store(true);
        return;
      }
      results.push_back(*v);
    });
  }
  EXPECT_THAT(results, ElementsAre(0, 1, 2, 3, 4));

  cq.Shutdown();
  for (auto& p : pool) {
    p.join();
  }

  // Attach a function to transform this source, the callbacks should happen in
  // the thread where the generator is running.

  // Attach a function to split the resulting source in many events, the
  // the callbacks continue to execute in that thread

  // Attach a sink that pushes the events to a `std::vector`

  // Start the process
}

TEST(ChannelTest, InFlight) {
  // A generator that uses a completion queue with a thread pool and a timer to
  // generate events every X milliseconds.

  // A queue that sends at most N elements to the sink at the same time.

  // A sink that takes 2 * X milliseconds to send the result to a vector.
}

TEST(ChannelTest, Queue) {
  // A generator that uses a completion queue with a thread pool and a timer to
  // generate events every X milliseconds.

  // A sink, this is a queue that keeps events so other threads can send them.

  // A sink that takes 2 * X milliseconds to send the result to a vector.
}

}  // namespace
}  // namespace internal
}  // namespace GOOGLE_CLOUD_CPP_NS
}  // namespace cloud
}  // namespace google
