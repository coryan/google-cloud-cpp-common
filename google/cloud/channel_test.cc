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
#include "google/cloud/optional.h"
#include "google/cloud/testing_util/chrono_literals.h"
#include <gmock/gmock.h>

namespace google {
namespace cloud {
inline namespace GOOGLE_CLOUD_CPP_NS {
namespace internal {
namespace {

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
  explicit channel_source(std::shared_ptr<Channel> c) : channel_(std::move(c)) {}

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

TEST(ChannelTest, Goal) {
  pipeline p = simple_source<int>{} | [](int x) { return std::to_string(x); } |
               transform_channel<std::string, std::string>(by_line) |
               keep_in_flight<std::string>(8) | simple_sink<std::string>{};
  p.start();
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

}  // namespace
}  // namespace internal
}  // namespace GOOGLE_CLOUD_CPP_NS
}  // namespace cloud
}  // namespace google
