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

TEST(ChannelTest, MakeSimpleChannel) {
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

TEST(ChannelTest, SimpleChannelTraits) {
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

TEST(ChannelTest, GeneratorBasic) {
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

TEST(ChannelTest, Transform) {
  int count = 0;
  auto gen = internal::generator([&count]() { return ++count; });
  auto tested = gen >> [](int x) { return 2 * x; } >>
                [](int x) { return std::to_string(x); };
  std::vector<std::string> actual;
  tested.on_data([&actual](std::string v) {
    actual.push_back(std::move(v));
    return actual.size() < 3 ? internal::on_data_resolution::kReschedule
                             : internal::on_data_resolution::kDone;
  });
  EXPECT_THAT(actual, ElementsAre("2", "4", "6"));

  static_assert(internal::concepts::is_source<decltype(gen)>::value,
                "The result of generator( should meet the is_source<S> "
                "requirements");
}

#if 0
TEST(ChannelTest, Goal) {
  ASSERT_TRUE(false) << "Code not ready yet";
  // Create a channel that can hold up to 10 elements, and does not restart
  // sending until it holds less than 5.
  auto input = internal::make_simple_channel_impl<int>(5, 10);

  // Apply some transformations to the output of this queue, note that these
  // do nothing for now, because there is nothing generating data on `input`.
  // run on the previously created thread.
  auto rx = std::move(input.rx) >> [](int x) { return 2 * x; } >>
            [](int x) { return std::to_string(x); };

  // Create a second channel to get the resulting strings.
  auto output = internal::make_simple_channel_impl<std::string>(0, 2);
  // Create a separate thread that pulls from the channel and stores into a
  // vector.  Note that we could use `foo::async` here to capture the result
  // into a future, but we are keeping this example simple.
  std::vector<std::string> actual;
  std::thread consumer(
      [&actual](decltype(output.rx) rx) {
        for (auto v = rx.pull(); v.has_value(); v = rx.pull()) {
          actual.push_back(*std::move(v));
        }
      },
      std::move(output.rx));
  // TODO() - connect the two queues.
  // rx >> std::move(output.tx);

  // Create a thread that pushes 1000 integers to this channel, blocking when
  // the channel is too busy.
  auto tx = std::move(input).tx;
  using ms = std::chrono::milliseconds;
  std::thread generator([&tx] {
    for (int i = 0; i != 1000; ++i) {
      tx.push(i);
      std::this_thread::sleep_for(ms(10));
    }
    tx.shutdown();
  });

  consumer.join();
  generator.join();

  std::vector<std::string> expected;
  int count = 0;
  std::generate_n(std::back_inserter(expected), 1000,
                  [&count] { return std::to_string(2 * count++); });
  EXPECT_THAT(actual, ElementsAre("2", "4", "6"));
}
#endif

}  // namespace
}  // namespace internal
}  // namespace GOOGLE_CLOUD_CPP_NS
}  // namespace cloud
}  // namespace google
