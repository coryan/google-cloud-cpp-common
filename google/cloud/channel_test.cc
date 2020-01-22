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
  future<optional<T>> async_pull_one() { return {}; }
  optional<T> pull_one() { return async_pull_one().get(); }
};

template <typename T>
class simple_sink {
 public:
  void shutdown() {}
  future<void> async_push_one(T) { return {}; }
};

template <typename T>
struct channel {
  simple_sink<T> tx;
  simple_source<T> rx;
};

struct pipeline {
  virtual void start() {}
};

template <typename T>
channel<T> keep_in_flight(int) {
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
simple_source<T> operator|(simple_source<T> s, channel<T> c) {
  auto p = s | std::move(c.tx);
  return bound_source<T>(std::move(p), std::move(c.rx));
}

TEST(ChannelTest, Goal) {
  pipeline p = simple_source<int>{} | [](int x) { return std::to_string(x); } |
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
