
#if __cplusplus < 20170101
#include "google/cloud/internal/invoke_result.h"
using google::cloud::internal::invoke_result_t ;
#else
#include <type_traits>
using std::invoke_result_t;
#endif


#include <deque>
#include <functional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

template <typename T>
class future ;

template <typename T> struct then_result {
  using type = T;
};
template <typename U> struct then_result<future<U>> {
  using type = U;
};

template <typename T>
using then_result_t = typename then_result<T>::type;

template <typename T>
class future {
 public:
  future() = default;

  T get() { return T{}; }

  template<
      typename F,
      typename R = then_result_t<invoke_result_t<F, future<T>>>
  >
  future<R> then(F /*continuation*/) { return {}; }
};

template<typename T>
future<T> make_ready_future(T);

template <class T>
class satisfied_future {
 public:
  T get();

  bool is_ready() const { return true; }

  template<
      typename F,
      typename R = then_result_t<invoke_result_t<F, satisfied_future<T>>>
  >
  satisfied_future<R> then(F /*continuation*/) { return {}; }
};

template <typename T>
class sink;

template <typename T>
class source {
 public:
  bool done() const { return false; }
  future<T> pull() { return {}; }

  future<void> terminate(sink<T> ) && { return {}; }
};

namespace pipelines_internal {

template<typename Transform, typename I, typename O, typename source_type>
class transformed_source {
 public:
  transformed_source(Transform t, source_type s)
  : transform_(std::move(t)), source_(std::move(s)) {}

  bool done() { return source_.done(); }

  using future_t = decltype(std::declval<transformed_source*>()->source_.pull());
  using pull_result_t = decltype(std::declval<future_t>().then([](future_t f) {
    return std::declval<Transform>(f.get());
  }));

  pull_result_t pull() {
    auto& transform = transform_;
    return source_.pull().then([transform](future_t f) {
      return transform(f.get());
    });
  }

 private:
  Transform transform_;
  source_type source_;
};

template<typename O, template <typename T> class promise_type,
template <typename T> class future_type>
class splicer_source {
 public:
  splicer_source()
       {}

  bool done() { return source_.done(); }

  class push_ready {
   public:
    void operator()(O value) {
    }

   private:
  };

  future_type<std::function<void(O)>> push() {
    return wait_for_not_full().then([this](future_type<void>) {
      return do_push();
    });
  }

  future_type<O> pull() {
    if (!ready_events_.empty()) {
      auto e = std::move(ready_events_.front());
      ready_events_.pop_front();
      return e;
    }
    return wait_for_not_empty().then([this](future_type<void>) {
      return pull();
    });
  }

 private:
  future_type<void> wait_for_not_empty() {
    waiting_for_not_empty_.promise_type<void> waiter;

  }
  future_type<void> wait_for_not_full() {

  }
  future_type<promise_type<O>> next_promise() {

  }

  std::deque<future_type<O>> ready_events_;
  std::deque<promise_type<O>> waiting_for_not_empty_;
  std::deque<promise_type<O>> waiting_for_not_full_;
};

}

namespace pipelines {

template <typename I, typename F,
  template <typename T> class source_type,
    typename O = invoke_result_t<F, I>>
class transform_pipeline {
 public:
  explicit transform_pipeline(F f) : transform_(std::move(f)) {}

  pipelines_internal::transformed_source<F, I, O, source_type<I>> operator()(source_type<I> s) {
    return pipelines_internal::transformed_source<F, I, O, source_type<I>>(std::move(s));
  }

 private:
  F transform_;
};

template <typename P1, typename P2>
class composed_pipeline {
 public:
  explicit composed_pipeline(P1 p1, P2 p2)
  : p1_(std::move(p1)), p2_(std::move(p2)) {}

  template <typename source_type>
  auto operator()(source_type s) -> decltype(std::declval<P2>()(std::declval<P1>()(s))) {
    return p2_(p1_(std::move(s)));
  }

 private:
  P1 p1_;
  P2 p2_;
};

template <typename I, typename F,
    template <typename T> class source_type,
    typename O = invoke_result_t<F, I>>
class splice_pipeline {
 public:
  explicit splice_pipeline(F f) : transform_(std::move(f)) {}

  pipelines_internal::transformed_source<F, I, O, source_type<I>> operator()(source_type<I> s) {
    return pipelines_internal::transformed_source<F, I, O, source_type<I>>(std::move(s));
  }

 private:
  F transform_;
};


}



template <typename T>
class sink {
 public:
  struct consume {
    void operator()(T /* value */) {}
    void operator()(std::exception_ptr /* ex */) {}
  };

  future<consume> push() { return make_ready_future<consume>({}); }
};

future<int> Adder(source<int> s) {
  if (s.done()) return make_ready_future(0);
  auto next = s.pull();
  struct recurse {
    source<int> s;

    future<int> operator()(future<int> value) {
      auto x = value.get();
      return Adder(std::move(s)).then([x](future<int> r) {
        return x + r.get();
      });
    }
  };
  return next.then(recurse{std::move(s)});
}

source<int> GetSource();
sink<std::string> GetSink();

future<void> GetPipeline() {
  auto source = GetSource();
  auto sink = GetSink();
  auto pipe = transform<int>([](int v) {
    return std::to_string(v);
  });

  return std::move(source).attach(pipe).terminate(sink);
}

