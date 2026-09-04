#pragma once

#include <concepts>
#include <expected>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>

#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/strand.hpp>
#include <asio/use_awaitable.hpp>

#include "aero/http/headers.hpp"
#include "aero/http/response.hpp"
#include "aero/urls/url.hpp"
#include "aero/websocket/close_code.hpp"
#include "aero/websocket/message.hpp"

namespace aero::websocket::concepts {

  using masking_key = std::array<std::byte, 4>;

  template <typename T>
  concept masking_key_source = requires(T obj) {
    { obj.next() } -> std::same_as<std::expected<masking_key, std::error_code>>;
  };

  template <typename Client>
  concept websocket_client =
    requires {
      typename Client::transport_type;
      typename Client::duration;
      typename Client::executor_type;
    } &&
    requires(Client client, std::string_view url_string, urls::url url, std::expected<urls::url, std::error_code> parsed_url,
      std::string header_name, std::string header_value, http::headers handshake_headers, std::string_view header_name_view,
      std::string_view text, std::span<const std::byte> bytes, websocket::close_code close_code, std::string_view close_reason,
      typename Client::duration timeout) {
      {
        client.async_connect(url_string, asio::as_tuple(asio::use_awaitable))
      } -> std::same_as<asio::awaitable<std::tuple<std::error_code, http::response>>>;

      {
        client.async_connect(parsed_url, asio::as_tuple(asio::use_awaitable))
      } -> std::same_as<asio::awaitable<std::tuple<std::error_code, http::response>>>;

      {
        client.async_connect(url, asio::as_tuple(asio::use_awaitable))
      } -> std::same_as<asio::awaitable<std::tuple<std::error_code, http::response>>>;

      {
        client.async_connect(url_string, handshake_headers, asio::as_tuple(asio::use_awaitable))
      } -> std::same_as<asio::awaitable<std::tuple<std::error_code, http::response>>>;

      {
        client.async_connect(parsed_url, handshake_headers, asio::as_tuple(asio::use_awaitable))
      } -> std::same_as<asio::awaitable<std::tuple<std::error_code, http::response>>>;

      {
        client.async_connect(url, handshake_headers, asio::as_tuple(asio::use_awaitable))
      } -> std::same_as<asio::awaitable<std::tuple<std::error_code, http::response>>>;

      {
        client.async_send_text(text, asio::as_tuple(asio::use_awaitable))
      } -> std::same_as<asio::awaitable<std::tuple<std::error_code>>>;

      {
        client.async_send_binary(bytes, asio::as_tuple(asio::use_awaitable))
      } -> std::same_as<asio::awaitable<std::tuple<std::error_code>>>;

      { client.async_ping(asio::as_tuple(asio::use_awaitable)) } -> std::same_as<asio::awaitable<std::tuple<std::error_code>>>;
      {
        client.async_ping(text, asio::as_tuple(asio::use_awaitable))
      } -> std::same_as<asio::awaitable<std::tuple<std::error_code>>>;
      {
        client.async_ping(bytes, asio::as_tuple(asio::use_awaitable))
      } -> std::same_as<asio::awaitable<std::tuple<std::error_code>>>;

      { client.async_pong(asio::as_tuple(asio::use_awaitable)) } -> std::same_as<asio::awaitable<std::tuple<std::error_code>>>;
      {
        client.async_pong(text, asio::as_tuple(asio::use_awaitable))
      } -> std::same_as<asio::awaitable<std::tuple<std::error_code>>>;
      {
        client.async_pong(bytes, asio::as_tuple(asio::use_awaitable))
      } -> std::same_as<asio::awaitable<std::tuple<std::error_code>>>;

      {
        client.async_close(close_code, asio::as_tuple(asio::use_awaitable))
      } -> std::same_as<asio::awaitable<std::tuple<std::error_code>>>;

      {
        client.async_close(close_code, close_reason, asio::as_tuple(asio::use_awaitable))
      } -> std::same_as<asio::awaitable<std::tuple<std::error_code>>>;

      {
        client.async_force_close(asio::as_tuple(asio::use_awaitable))
      } -> std::same_as<asio::awaitable<std::tuple<std::error_code>>>;

      {
        client.async_read(asio::as_tuple(asio::use_awaitable))
      } -> std::same_as<asio::awaitable<std::tuple<std::error_code, websocket::message>>>;

      { client.connect(url, handshake_headers) } -> std::same_as<std::tuple<std::error_code, http::response>>;
      { client.connect(parsed_url, handshake_headers) } -> std::same_as<std::tuple<std::error_code, http::response>>;
      { client.connect(url_string, handshake_headers) } -> std::same_as<std::tuple<std::error_code, http::response>>;

      { client.send_text(text) } -> std::same_as<std::error_code>;
      { client.send_binary(bytes) } -> std::same_as<std::error_code>;

      { client.ping() } -> std::same_as<std::error_code>;
      { client.ping(text) } -> std::same_as<std::error_code>;
      { client.ping(bytes) } -> std::same_as<std::error_code>;

      { client.pong() } -> std::same_as<std::error_code>;
      { client.pong(text) } -> std::same_as<std::error_code>;
      { client.pong(bytes) } -> std::same_as<std::error_code>;

      { client.close(close_code) } -> std::same_as<std::error_code>;
      { client.close(close_code, close_reason) } -> std::same_as<std::error_code>;
      { client.force_close() } -> std::same_as<std::error_code>;

      { client.read() } -> std::same_as<std::expected<websocket::message, std::error_code>>;
      { client.read(timeout) } -> std::same_as<std::expected<websocket::message, std::error_code>>;

      { client.is_open_for_writing() } -> std::same_as<bool>;
      { client.is_connecting() } -> std::same_as<bool>;
      { client.is_closed() } -> std::same_as<bool>;
      { client.is_closing() } -> std::same_as<bool>;
      { client.is_transport_secure() } -> std::same_as<bool>;

      { client.get_executor() } -> std::same_as<typename Client::executor_type>;
      { client.get_strand() } -> std::same_as<asio::strand<typename Client::executor_type>>;
    };

} // namespace aero::websocket::concepts
