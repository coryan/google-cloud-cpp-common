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

#include "google/cloud/internal/invoke_result.h"
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

template <typename Source, typename Sink>
class synchronous_pipeline {
 public:
  synchronous_pipeline(Source source, Sink sink)
      : source_(std::move(source)), sink_(std::move(sink)) {}

  void start() && {
    for (auto t = source_.pull(); t.has_value(); t = source_.pull()) {
      sink_(*std::move(t));
    }
    sink_.shutdown();
  }

 private:
  Source source_;
  Sink sink_;
};

template <typename Source, typename Sink>
auto connect_synchronous(Source&& source, Sink&& sink)
    -> synchronous_pipeline<typename std::decay<Source>::type,
                            typename std::decay<Sink>::type> {
  return synchronous_pipeline<typename std::decay<Source>::type,
                              typename std::decay<Sink>::type>(
      std::forward<Source>(source), std::forward<Sink>(sink));
}

template <typename Generator, typename T = invoke_result_t<Generator>>
class generate_n_source {
 public:
  using event_type = T;

  explicit generate_n_source(std::size_t count, Generator g)
      : count_(count), generator_(std::move(g)) {}

  optional<T> pull() {
    if (count_ == 0) return {};
    --count_;
    return generator_();
  }

  template <typename Sink>
  auto connect(Sink&& sink) && -> decltype(
      connect_synchronous(std::move(*this), std::forward<Sink>(sink))) {
    return connect_synchronous(std::move(*this), std::forward<Sink>(sink));
  }

 private:
  std::size_t count_;
  Generator generator_;
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

  void start() && {
    auto tmp = std::move(impl_);
    tmp->start();
  }

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
  using event_type = T;

  source() = default;
  source(source const&) = delete;
  source& operator=(source const&) = delete;
  source(source&&) noexcept = default;
  source& operator=(source&&) noexcept = default;

  optional<T> pull() { return impl_->pull(); }
  pipeline connect(sink<T> s) && {
    auto tmp = std::move(impl_);
    return pipeline(tmp->connect(std::move(s.impl_)));
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
