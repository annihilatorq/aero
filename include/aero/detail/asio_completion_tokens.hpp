#pragma once

#include <type_traits>

#include <asio/default_completion_token.hpp>
#include <asio/execution/executor.hpp>
#include <asio/strand.hpp>

namespace aero::detail {

  template <typename Executor>
  struct default_token_strand : asio::strand<Executor> {
    using asio::strand<Executor>::strand;
    using default_completion_token_type = asio::default_completion_token_t<Executor>;

    explicit default_token_strand(asio::strand<Executor> executor): asio::strand<Executor>(executor) {}
  };

  template <typename T>
  using default_token_type = typename asio::default_completion_token<
    typename std::conditional_t<asio::execution::executor<T>, T, typename std::remove_cvref_t<T>::executor_type>>::type;

  template <typename T>
  [[nodiscard]] constexpr auto default_token() {
    return default_token_type<T>();
  }

} // namespace aero::detail
