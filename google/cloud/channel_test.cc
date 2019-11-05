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

}  // namespace
}  // namespace internal
}  // namespace GOOGLE_CLOUD_CPP_NS
}  // namespace cloud
}  // namespace google
