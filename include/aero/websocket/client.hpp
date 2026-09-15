#pragma once

#include "aero/websocket/basic_connection.hpp"
#include "aero/websocket/detail/concepts.hpp"
#include "aero/websocket/role.hpp"
#include <asio/any_io_executor.hpp>
#include <asio/execution/executor.hpp>

namespace aero::websocket {

  template <asio::execution::executor Executor>
  using basic_client = websocket::basic_connection<websocket::role::client, Executor>;
  using client = basic_client<asio::any_io_executor>;

  static_assert(websocket::concepts::websocket_client<client>);

} // namespace aero::websocket
