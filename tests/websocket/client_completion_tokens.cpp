#include <chrono>
#include <concepts>
#include <string_view>
#include <tuple>
#include <ut/ut.hpp>

#include <asio/as_tuple.hpp>
#include <asio/cancel_after.hpp>
#include <asio/default_completion_token.hpp>
#include <asio/deferred.hpp>
#include <asio/io_context.hpp>
#include <asio/redirect_error.hpp>
#include <asio/use_awaitable.hpp>
#include <asio/use_future.hpp>

#include "aero/http/response.hpp"
#include "aero/websocket/client.hpp"
#include "aero/websocket/message.hpp"

using namespace ut;

template <typename Client>
void compile_token_matrix() {
  using namespace std::chrono_literals;

  using response_awaitable = asio::awaitable<std::tuple<std::error_code, aero::http::response>>;
  using error_awaitable = asio::awaitable<std::tuple<std::error_code>>;
  using message_awaitable = asio::awaitable<std::tuple<std::error_code, aero::websocket::message>>;

  constexpr auto as_tuple_awaitable = asio::as_tuple(asio::use_awaitable);

  static_assert(
    std::same_as<decltype(std::declval<Client&>().async_connect(std::string_view{}, as_tuple_awaitable)), response_awaitable>);

  static_assert(std::same_as<decltype(std::declval<Client&>().async_connect(std::string_view{},
                               asio::cancel_after(1s, as_tuple_awaitable))),
    response_awaitable>);

  static_assert(
    std::same_as<decltype(std::declval<Client&>().async_send_text(std::string_view{}, as_tuple_awaitable)), error_awaitable>);

  static_assert(std::same_as<decltype(std::declval<Client&>().async_read(as_tuple_awaitable)), message_awaitable>);

  std::error_code send_ec{};
  static_assert(std::same_as<decltype(std::declval<Client&>().async_send_text(std::string_view{},
                               asio::redirect_error(asio::use_awaitable, send_ec))),
    asio::awaitable<void>>);

  static_assert(
    std::same_as<decltype(std::declval<Client&>().async_send_text(std::string_view{}, asio::use_future)), std::future<void>>);

  static_assert(std::same_as<decltype(std::declval<Client&>().async_connect(std::string_view{}, asio::use_future)),
    std::future<aero::http::response>>);
}

int main() {
  using namespace std::chrono_literals;
  using aero::websocket::client;
  using aero::websocket::coro_client;
  using message_signature = void(std::error_code, aero::websocket::message);
  using response_awaitable = asio::awaitable<std::tuple<std::error_code, aero::http::response>>;
  using message_awaitable = asio::awaitable<std::tuple<std::error_code, aero::websocket::message>>;

  suite websocket_client_completion_tokens = [] {
    "compiles the supported completion token matrix"_test = [] {
      compile_token_matrix<aero::websocket::client>();
    };
  };

  suite websocket_client_default_completion_tokens = [] {
    "client defaults to deferred with the plain signature"_test = [] {
      static_assert(std::same_as<asio::default_completion_token_t<client::executor_type>, asio::deferred_t>);
      static_assert(
        std::same_as<asio::completion_signature_of_t<decltype(std::declval<client&>().async_read())>, message_signature>);
    };

    "coro_client defaults to as_tuple over use_awaitable"_test = [] {
      static_assert(std::same_as<asio::default_completion_token_t<coro_client::executor_type>,
        asio::as_tuple_t<asio::use_awaitable_t<asio::any_io_executor>>>);
      static_assert(std::same_as<decltype(std::declval<coro_client&>().async_read()), message_awaitable>);
      static_assert(std::same_as<decltype(std::declval<coro_client&>().async_connect(std::string_view{})), response_awaitable>);
    };

    "partial cancel_after keeps the default token"_test = [] {
      static_assert(std::same_as<decltype(std::declval<coro_client&>().async_read(asio::cancel_after(1s))), message_awaitable>);
      static_assert(
        std::same_as<decltype(std::declval<coro_client&>().async_connect(std::string_view{}, asio::cancel_after(1s))),
          response_awaitable>);
      static_assert(
        std::same_as<asio::completion_signature_of_t<decltype(std::declval<client&>().async_read(asio::cancel_after(1s)))>,
          message_signature>);
    };

    "partial as_tuple on client wraps the signature in a tuple"_test = [] {
      static_assert(std::same_as<asio::completion_signature_of_t<decltype(std::declval<client&>().async_read(asio::as_tuple))>,
        void(std::tuple<std::error_code, aero::websocket::message>)>);
    };

    "basic_coro_client with io_context executor awaits a tuple"_test = [] {
      using executor = asio::io_context::executor_type;
      using typed_client = aero::websocket::basic_coro_client<executor>;
      using typed_message_awaitable = asio::awaitable<std::tuple<std::error_code, aero::websocket::message>, executor>;

      static_assert(std::same_as<asio::default_completion_token_t<typed_client::executor_type>,
        asio::as_tuple_t<asio::use_awaitable_t<executor>>>);
      static_assert(std::same_as<decltype(std::declval<typed_client&>().async_read()), typed_message_awaitable>);
      static_assert(
        std::same_as<decltype(std::declval<typed_client&>().async_read(asio::cancel_after(1s))), typed_message_awaitable>);

      asio::io_context io;
      typed_client typed{io.get_executor()};
      expect(typed.get_executor() == io.get_executor());
    };
  };
}
