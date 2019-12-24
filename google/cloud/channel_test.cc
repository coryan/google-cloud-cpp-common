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

#include "google/cloud/testing_util/chrono_literals.h"
#include <gmock/gmock.h>

namespace google {
namespace cloud {
inline namespace GOOGLE_CLOUD_CPP_NS {
namespace internal {
namespace {

class in_flight_connector {
 public:
  void start(int n) {
    for (int i = 0; i != n; ++i) done();
  }

  void send_one(optional<std::string> v) {
    if (!v.has_value()) shutdown();
    output_.async_push_one(*std::move(v)).then([this](future<void>) {
      done();
    });
  }

  void shutdown() {
    output_.shutdown();
  }

 private:
  void done() {
    input_.async_pull_one().then([this](future<optional<std::string>> f) {
      send_one(f.get());
    });
  }

  internal::simple_channel<std::string> input_;
  internal::simple_channel<std::string> output_;
};

TEST(ChannelTest, Goal) {
  using ms = std::chrono::milliseconds;

  auto endpoints = internal::make_simple_channel<int>();
  auto tx = std::move(endpoints.tx);
  auto rx = std::move(endpoints.rx);

  std::thread generator([&tx] {
    for(int i = 0; i != 10; ++i) {
      tx.push(i);
      std::this_thread::sleep_for(ms(10));
    }
  });

  rx | [](int x) { return std::to_string(x); } |



  generator.join();
}

}  // namespace
}  // namespace internal
}  // namespace GOOGLE_CLOUD_CPP_NS
}  // namespace cloud
}  // namespace google
