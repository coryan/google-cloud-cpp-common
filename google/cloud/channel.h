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

#include "google/cloud/future.h"
#include "google/cloud/internal/invoke_result.h"
#include "google/cloud/optional.h"
#include "google/cloud/version.h"
#include <deque>
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

enum class channel_state {
  kAccepting,
  kDraining,
  kShutdown,
};

/**
 * A thread-safe queue with high and low watermarks to control flow.
 */
template <typename T>
class simple_channel : public std::enable_shared_from_this<simple_channel<T>> {
 public:
  simple_channel() = default;
  simple_channel(std::size_t lwm, std::size_t hwm) : lwm_(lwm), hwm_(hwm) {}

  void shutdown() {
    std::unique_lock<std::mutex> lk(mu_);
    if (state_ != channel_state::kAccepting) {
      // Already draining or shutdown.
      return;
    }
    state_ =
        buffer_.empty() ? channel_state::kShutdown : channel_state::kDraining;
    if (!sink_) {
      return;
    }
    lk.unlock();
    sink_->shutdown();
  }

  optional<T> pull() {
    std::unique_lock<std::mutex> lk(mu_);
    pull_wait_.wait(lk, [this] {
      return !buffer_.empty() || state_ == channel_state::kShutdown;
    });
    auto value = pop(lk);
    lk.unlock();
    return value;
  }

  void push(T value) {
    std::unique_lock<std::mutex> lk(mu_);
    if (state_ != channel_state::kAccepting) {
      return;
    }
    if (!sink_) {
      push_wait_.wait(lk, [this] { return buffer_.size() < lwm_; });
      buffer_.push_back(std::move(value));
      pull_wait_.notify_all();
      return;
    }
    lk.unlock();
    sink_->push(std::move(value));
  }

  void drain(std::unique_ptr<sink_impl<T>> sink) {
    // Drain any queued messages, afterwards all messages will be pushed
    // automatically.
    std::unique_lock<std::mutex> lk(mu_);
    while (!buffer_.empty()) {
      T value = std::move(buffer_.front());
      buffer_.pop_front();
      lk.unlock();
      sink_->push(std::move(value));
      lk.lock();
    }
    sink_ = std::move(sink);
  }

  struct pipeline {
   public:
    explicit pipeline(std::shared_ptr<simple_channel> channel,
                      std::unique_ptr<sink_impl<T>> sink)
        : channel_(std::move(channel)), sink_(std::move(sink)) {}

    void start() { channel_->drain(std::move(sink_)); }

   private:
    std::shared_ptr<simple_channel> channel_;
    std::unique_ptr<sink_impl<T>> sink_;
  };

  template <typename Sink>
  void connect(Sink&& sink) && {
    struct type_erased : public sink_impl<T> {
      explicit type_erased(Sink&& sink) : sink_(std::move(sink)) {}

      void shutdown() override { sink_.shutdown(); }
      void push(T value) override { sink_.push(std::move(value)); }

      typename std::decay<Sink>::type sink_;
    };
    return pipeline(this->shared_from_this(),
                    make_unique<type_erased>(std::forward<Sink>(sink)));
  }

  class source {
   public:
    using event_type = T;
    explicit source(std::shared_ptr<simple_channel> channel)
        : channel_(std::move(channel)) {}

    optional<T> pull() { return channel_->pull(); }

    template <typename Sink>
    void connect(Sink&& sink) && {
      return channel_->connect(std::forward<Sink>(sink));
    }

   private:
    std::shared_ptr<simple_channel> channel_;
  };

  class sink {
   public:
    explicit sink(std::shared_ptr<simple_channel> channel)
        : channel_(std::move(channel)) {}

    void push(T value) { channel_->push(std::move(value)); }
    void shutdown() { channel_->shutdown(); }

   private:
    std::shared_ptr<simple_channel> channel_;
  };

  struct endpoints {
    sink tx;
    source rx;
  };

 private:
  optional<T> pop(std::unique_lock<std::mutex>&) {
    if (state_ == channel_state::kShutdown) {
      return {};
    }
    auto value = std::move(buffer_.front());
    buffer_.pop_front();
    if (state_ == channel_state::kDraining && buffer_.size() < lwm_) {
      state_ = channel_state::kAccepting;
      push_wait_.notify_all();
    }
    return value;
  }

  std::mutex mu_;
  std::condition_variable push_wait_;
  std::condition_variable pull_wait_;
  std::deque<T> buffer_;
  std::unique_ptr<sink_impl<T>> sink_;
  std::size_t lwm_ = std::numeric_limits<std::size_t>::max() - 1;
  std::size_t hwm_ = std::numeric_limits<std::size_t>::max();
  channel_state state_ = channel_state::kAccepting;
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
