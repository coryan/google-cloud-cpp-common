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

#include <absl/types/variant.h>
#include <gmock/gmock.h>

struct PrintVisitor {
  void operator()(absl::monostate) {
    std::cout << "monostate"
              << "\n";
  }
  void operator()(int x) { std::cout << "int = " << x << "\n"; }
  void operator()(std::string const& x) {
    std::cout << "string = <" << x << ">\n";
  }
};

TEST(AbslVariantTest, Basic) {
  using tested = absl::variant<absl::monostate, int, std::string>;

  std::vector<tested> values{{}, 5, "foo", 42, "bar"};

  PrintVisitor visitor;
  for (auto const& x : values) {
    absl::visit(visitor, x);
  }
}
