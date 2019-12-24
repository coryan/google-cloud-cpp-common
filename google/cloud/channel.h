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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_CHANNEL_H_
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_CHANNEL_H_

#include "google/cloud/optional.h"
#include "google/cloud/version.h"
#include <memory>

namespace google {
namespace cloud {
inline namespace GOOGLE_CLOUD_CPP_NS {
namespace internal {

class pipeline_impl {
 public:
  virtual ~pipeline_impl() = default;

  virtual void start() = 0;
};

template <typename T>
class sink_impl {
 public:
  virtual ~sink_impl() = default;

  virtual void push(T) = 0;
  virtual void shutdown() = 0;
};

template <typename T>
class source_impl {
 public:
  virtual ~source_impl() = default;

  virtual optional<T> pull() = 0;
  virtual std::unique_ptr<pipeline_impl> connect(
      std::unique_ptr<sink_impl<T>>) = 0;
};

}  // namespace internal

template <typename T>
class sink;
template <typename T>
class source;

class pipeline {
 public:
  pipeline() = default;
  pipeline(pipeline const&) = delete;
  pipeline& operator=(pipeline const&) = delete;
  pipeline(pipeline&&) = default;
  pipeline& operator=(pipeline&&) = default;

  void start() { impl_->start(); }

 private:
  template <typename T>
  friend class source;

  explicit pipeline(std::unique_ptr<internal::pipeline_impl> impl)
      : impl_(std::move(impl)) {}

  std::unique_ptr<internal::pipeline_impl> impl_;
};

template <typename T>
class sink {
 public:
  sink() = default;
  sink(sink const&) = delete;
  sink& operator=(sink const&) = delete;
  sink(sink&&) noexcept = default;
  sink& operator=(sink&&) noexcept = default;

  void push(T value) { impl_->push(std::move(value)); }
  void shutdown() { impl_->shutdown(); }

 private:
  friend class source<T>;

  explicit sink(std::unique_ptr<internal::sink_impl<T>> impl)
      : impl_(std::move(impl)) {}

  std::unique_ptr<internal::sink_impl<T>> impl_;
};

template <typename T>
class source {
 public:
  source() = default;
  source(source const&) = delete;
  source& operator=(source const&) = delete;
  source(source&&) noexcept = default;
  source& operator=(source&&) noexcept = default;

  optional<T> pull() { return impl_->pull(); }
  pipeline connect(sink<T> s) {
    return pipeline(impl_->connect(std::move(s.impl_)));
  }

 private:
  explicit source(std::unique_ptr<internal::source_impl<T>> impl)
      : impl_(std::move(impl)) {}

  std::unique_ptr<internal::source_impl<T>> impl_;
};

}  // namespace GOOGLE_CLOUD_CPP_NS
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_CHANNEL_H_
