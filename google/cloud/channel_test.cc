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

#include "google/cloud/channel.h"
#include "google/cloud/testing_util/chrono_literals.h"
#include <gmock/gmock.h>

namespace google {
namespace cloud {
inline namespace GOOGLE_CLOUD_CPP_NS {
namespace internal {
namespace {

using ::testing::ElementsAre;

TEST(FutureQueueTest, Basic) {
  buffered_channel<std::string> tested;
  tested.push(future_state<std::string>{"foo"});
  tested.push(future_state<std::string>{"bar"});
  tested.push(future_state<std::string>{"baz"});
  std::vector<std::string> actual{
      absl::get<std::string>(tested.pull()),
      absl::get<std::string>(tested.pull()),
      absl::get<std::string>(tested.pull()),
  };
  EXPECT_THAT(actual, ElementsAre("foo", "bar", "baz"));
}

TEST(FutureQueueTest, MakeBufferedChannel) {
  std::shared_ptr<sink_impl<std::string>> tx;
  std::shared_ptr<source_impl<std::string>> rx;
  std::tie(tx, rx) = make_buffered_channel_impl<std::string>();

  tx->push("foo");
  tx->push("bar");
  tx->push("baz");

  std::vector<std::string> actual{
      *rx->pull(),
      *rx->pull(),
      *rx->pull(),
  };
  EXPECT_THAT(actual, ElementsAre("foo", "bar", "baz"));
}

TEST(FutureQueueTest, MakeSimpleChannel) {
  auto endpoints = make_simple_channel_impl<std::string>();
  auto tx = std::move(endpoints.tx);
  auto rx = std::move(endpoints.rx);

  tx.push("foo");
  tx.push("bar");
  tx.push("baz");

  std::vector<std::string> actual{
      *rx.pull(),
      *rx.pull(),
      *rx.pull(),
  };
  EXPECT_THAT(actual, ElementsAre("foo", "bar", "baz"));
}

TEST(FutureQueueTest, SimpleChannelTraits) {
  using tested = internal::simple_channel<int>;
  static_assert(internal::concepts::is_sink<tested::sink, int>::value,
                "A simple_channel<int>::sink should meet the is_sink<S,int> "
                "requirements");

  static_assert(
      !internal::concepts::is_source<tested::sink>::value,
      "A simple_channel<int>::sink should *NOT* meet the is_source<S,int> "
      "requirements");

  static_assert(
      internal::concepts::is_source<tested::source>::value,
      "A simple_channel<int>::source should meet the is_source<S,int> "
      "requirements");
}

TEST(FutureQueueTest, GeneratorBasic) {
  auto tested = internal::generator([]() -> int { return 42; });
  EXPECT_EQ(42, *tested.pull());

  std::vector<int> actual;
  tested.on_data([&actual](int v) {
    actual.push_back(v);
    return actual.size() < 3 ? internal::on_data_resolution::kReschedule
                             : internal::on_data_resolution::kDone;
  });
  EXPECT_THAT(actual, ElementsAre(42, 42, 42));

  static_assert(internal::concepts::is_source<decltype(tested)>::value,
                "The result of generator() should meet the is_source<S> "
                "requirements");
}

TEST(FutureQueueTest, Transform) {
  int count = 0;
  auto gen = internal::generator([&count]() { return ++count; });

  auto tested = gen >>= [](int x) { return std::to_string(x); };
  std::vector<std::string> actual;
  tested.on_data([&actual](std::string v) {
    actual.push_back(std::move(v));
    return actual.size() < 3 ? internal::on_data_resolution::kReschedule
                             : internal::on_data_resolution::kDone;
  });
  EXPECT_THAT(actual, ElementsAre("1", "2", "3"));

  static_assert(internal::concepts::is_source<decltype(gen)>::value,
                "The result of generator( should meet the is_source<S> "
                "requirements");
}

}  // namespace
}  // namespace internal
}  // namespace GOOGLE_CLOUD_CPP_NS
}  // namespace cloud
}  // namespace google
