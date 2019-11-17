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

TEST(ChannelTest, ConceptsHasPush) {
  struct expect_false {};
  struct expect_true {
    void push(int);
  };
  EXPECT_FALSE((concepts::has_push<expect_false, int>::value));
  EXPECT_FALSE((concepts::has_push<expect_false, std::string>::value));
  EXPECT_TRUE((concepts::has_push<expect_true, int>::value));
}

TEST(ChannelTest, ConceptsHasShutdown) {
  struct missing_shutdown {};
  struct mismatch_shutdown_type {};
  struct expect_true {
    void shutdown();
  };
  EXPECT_FALSE((concepts::has_shutdown<missing_shutdown>::value));
  EXPECT_FALSE((concepts::has_shutdown<mismatch_shutdown_type>::value));
  EXPECT_TRUE((concepts::has_shutdown<expect_true>::value));
}

TEST(ChannelTest, ConceptsIsSink) {
  struct expect_false {};
  struct expect_true {
    void push(int);
    void shutdown();
  };
  EXPECT_FALSE((concepts::is_sink<expect_false, int>::value));
  EXPECT_FALSE((concepts::is_sink<expect_true, std::string>::value));
  EXPECT_TRUE((concepts::is_sink<expect_true, int>::value));
}

TEST(ChannelTest, ConceptsHasPull) {
  struct expect_false {};
  struct expect_true {
    void pull();
  };
  EXPECT_FALSE((concepts::has_pull<expect_false, int>::value));
  EXPECT_TRUE((concepts::has_pull<expect_true, int>::value));
}

struct has_on_data_generic {
  template <typename Functor>
  void on_data(Functor&&) {}
};

TEST(ChannelTest, ConceptsOnData) {
  struct expect_false {};
  struct mismatched_type {
    void on_data(int);
  };
  struct expect_true {
    void on_data(std::function<void(int)>);
  };
  EXPECT_FALSE((concepts::has_on_data<expect_false, int>::value));
  EXPECT_FALSE((concepts::has_on_data<mismatched_type, int>::value));
  EXPECT_TRUE((concepts::has_on_data<expect_true, int>::value));
  EXPECT_TRUE((concepts::has_on_data<has_on_data_generic, int>::value));
}

TEST(ChannelTest, ConceptsEventType) {
  struct expect_false {};
  struct expect_true {
    using event_type = int;
  };
  EXPECT_FALSE((concepts::has_event_type<expect_false>::value));
  EXPECT_TRUE((concepts::has_event_type<expect_true>::value));
}

TEST(ChannelTest, ConceptsIsSource) {
  struct expect_false_0 {};
  struct expect_false_1 {
    optional<int> pull();
  };
  struct expect_false_2 {
    void on_data(std::function<void(int)>);
    optional<int> pull();
  };
  struct expect_false_3 {
    using event_type = int;
    void on_data(std::function<void(int)>);
  };
  struct expect_true {
    using event_type = int;
    void on_data(std::function<void(int)>);
    optional<int> pull();
  };
  EXPECT_FALSE((concepts::is_source<expect_false_0>::value));
  EXPECT_FALSE((concepts::is_source<expect_false_1>::value));
  EXPECT_FALSE((concepts::is_source<expect_false_2>::value));
  EXPECT_FALSE((concepts::is_source<expect_false_3>::value));
  EXPECT_TRUE((concepts::is_source<expect_true>::value));
}

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
