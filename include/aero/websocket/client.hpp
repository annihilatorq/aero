#pragma once

#include "aero/websocket/basic_connection.hpp"
#include "aero/websocket/detail/concepts.hpp"
#include "aero/websocket/role.hpp"
#include <asio/any_io_executor.hpp>
#include <asio/as_tuple.hpp>
#include <asio/execution/executor.hpp>
#include <asio/use_awaitable.hpp>

namespace aero::websocket {

  template <asio::execution::executor Executor>
  using basic_client = websocket::basic_connection<websocket::role::client, Executor>;
  using client = basic_client<asio::any_io_executor>;

  template <asio::execution::executor Executor>
  using basic_coro_client = asio::as_tuple_t<asio::use_awaitable_t<Executor>>::template as_default_on_t<basic_client<Executor>>;
  using coro_client = basic_coro_client<asio::any_io_executor>;

  static_assert(websocket::concepts::websocket_client<client>);
  static_assert(websocket::concepts::websocket_client<coro_client>);

} // namespace aero::websocket
