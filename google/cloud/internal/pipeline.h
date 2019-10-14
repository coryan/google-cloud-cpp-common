
#if __cplusplus < 20170101
#include "google/cloud/internal/invoke_result.h"
using google::cloud::internal::invoke_result_t ;
#else
#include <type_traits>
using std::invoke_result_t;
#endif


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
 private:
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

template <typename I, typename O>
class pipeline {
 public:
};

template <typename I, typename F,
    typename O = invoke_result_t<F, I>>
pipeline<I, O> transform(F ) { return {}; }

template <typename T>
class sink;

template <typename T>
class source {
 public:
  bool done() const { return false; }

  future<T> pull() { return {}; }

  template <typename O>
  source<O> attach(pipeline<T, O> ) && { return {}; }

  future<void> terminate(sink<T> ) && { return {}; }
};

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

