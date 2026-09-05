#pragma once

#include <atomic>
#include <chrono>
#include <expected>
#include <future>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>

#include <asio/as_tuple.hpp>
#include <asio/async_result.hpp>
#include <asio/bind_allocator.hpp>
#include <asio/buffer.hpp>
#include <asio/cancel_after.hpp>
#include <asio/cancellation_state.hpp>
#include <asio/co_composed.hpp>
#include <asio/co_spawn.hpp>
#include <asio/error.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read_until.hpp>
#include <asio/steady_timer.hpp>
#include <asio/strand.hpp>
#include <asio/use_awaitable.hpp>
#include <asio/use_future.hpp>

#include "aero/default_executor.hpp"
#include "aero/detail/aligned_allocator.hpp"
#include "aero/error.hpp"
#include "aero/http/detail/line_endings.hpp"
#include "aero/http/response.hpp"
#include "aero/net/transport.hpp"
#include "aero/tls/error.hpp"
#include "aero/urls/error.hpp"
#include "aero/urls/url.hpp"
#include "aero/util/deadline.hpp"
#include "aero/util/final_action.hpp"
#include "aero/util/string.hpp"
#include "aero/websocket/client_handshaker.hpp"
#include "aero/websocket/close_code.hpp"
#include "aero/websocket/connection_options.hpp"
#include "aero/websocket/detail/client_frame_builder.hpp"
#include "aero/websocket/detail/message_reader.hpp"
#include "aero/websocket/error.hpp"
#include "aero/websocket/message.hpp"
#include "aero/websocket/port.hpp"
#include "aero/websocket/role.hpp"
#include "aero/websocket/state.hpp"

#if AERO_USE_TLS
#include "aero/tls/detail/default_context.hpp"
#include <asio/ssl/context.hpp>
#endif

namespace aero::websocket {

  template <websocket::role Role>
  class basic_connection {
    using protocol_error = websocket::protocol_error;
    constexpr static std::span<const std::byte> null_bytes{};
    constexpr static std::chrono::seconds default_close_timeout{5};
    constexpr static std::chrono::seconds transport_drain_deadline{1};

   public:
    using transport_type = aero::net::transport;
    using duration = std::chrono::steady_clock::duration;
    using executor_type = typename transport_type::executor_type;

    explicit basic_connection(connection_options options = {})
      : strand_(asio::make_strand(aero::get_default_executor())),
        client_frame_builder_({.validate_utf8 = options.validate_outgoing_utf8}),
        message_reader_({.max_message_size = options.max_message_size}),
        client_handshaker_(options.client_handshaker),
        max_read_buffer_size_(options.read_buffer_size) {}

    explicit basic_connection(executor_type executor, connection_options options = {})
      : strand_(asio::make_strand(executor)),
        client_frame_builder_({.validate_utf8 = options.validate_outgoing_utf8}),
        message_reader_({.max_message_size = options.max_message_size}),
        client_handshaker_(options.client_handshaker),
        max_read_buffer_size_(options.read_buffer_size) {}

#if AERO_USE_TLS
    explicit basic_connection(asio::ssl::context& ssl_ctx): basic_connection(connection_options{}, ssl_ctx) {}

    explicit basic_connection(connection_options options, asio::ssl::context& ssl_ctx): basic_connection(std::move(options)) {
      ssl_ctx_ = std::addressof(ssl_ctx); // NOLINT(cppcoreguidelines-prefer-member-initializer)
    }

    explicit basic_connection(executor_type executor, asio::ssl::context& ssl_ctx)
      : basic_connection(executor, connection_options{}, ssl_ctx) {}

    explicit basic_connection(executor_type executor, connection_options options, asio::ssl::context& ssl_ctx)
      : basic_connection(executor, std::move(options)) {
      ssl_ctx_ = std::addressof(ssl_ctx); // NOLINT(cppcoreguidelines-prefer-member-initializer)
    }
#endif

    template <typename CompletionToken>
    auto async_connect(std::expected<urls::url, std::error_code> parsed_url, http::headers headers, CompletionToken&& token) {
      auto bound_token = asio::bind_allocator(aero::detail::aligned_allocator<>{}, std::forward<CompletionToken>(token));

      return asio::async_initiate<decltype(bound_token), void(std::error_code, http::response)>(
        asio::co_composed<void(std::error_code, http::response)>(
          [](auto, basic_connection* self, std::expected<urls::url, std::error_code> parsed_url, http::headers headers)
            -> void {
            if (!self->is_current_state(state::closed)) {
              co_return {protocol_error::connection_not_closed, http::response{}};
            }

            if (!parsed_url.has_value()) {
              co_return {parsed_url.error(), http::response{}};
            }

            const urls::url& url = *parsed_url;

            if (auto ec = validate_websocket_url(url); ec) {
              co_return {ec, http::response{}};
            }

            auto port = websocket::get_port_for_scheme(url);
            if (!port) {
              co_return {port.error(), http::response{}};
            }

            if (auto ec = self->construct_transport(aero::striequal(url.scheme(), "wss")); ec) {
              co_return {ec, http::response{}};
            }

            self->reset_connection_state(state::connecting);

            auto [connect_ec] =
              co_await self->transport_->async_connect(std::string{url.host()}, *port, return_as_deferred_tuple());
            if (connect_ec) {
              co_await self->async_finalize_session({}, return_as_deferred_tuple());
              co_return {connect_ec, http::response{}};
            }

            // Build bodyless HTTP websocket upgrade request
            auto handshake = self->client_handshaker_.build_request(url, std::move(headers));
            if (!handshake) {
              co_await self->async_finalize_session({}, return_as_deferred_tuple());
              co_return {handshake.error(), http::response{}};
            }

            auto [write_ec, bytes_written] =
              co_await self->transport_->async_write(handshake->bytes(), return_as_deferred_tuple());
            if (write_ec) {
              co_await self->async_finalize_session({}, return_as_deferred_tuple());
              co_return {write_ec, http::response{}};
            }

            std::vector<std::byte> response_buffer;

            // Read server response until "\r\n\r\n"
            auto [read_ec, bytes_read] = co_await asio::async_read_until(*self->transport_,
              asio::dynamic_buffer(response_buffer),
              http::detail::double_crlf,
              return_as_deferred_tuple());
            if (read_ec) {
              co_await self->async_finalize_session({}, return_as_deferred_tuple());
              co_return {read_ec, http::response{}};
            }

            // https://www.boost.org/doc/libs/1_43_0/doc/html/boost_asio/reference/async_read_until.html
            // "After a successful async_read_until operation, the streambuf
            // may contain additional data beyond the delimiter"
            const bool buffer_has_data_after_delimiter = response_buffer.size() > bytes_read;
            if (buffer_has_data_after_delimiter) {
              auto data_after_handshake = std::span{response_buffer}.subspan(bytes_read);
              self->data_received_in_handshake_ = std::vector{std::from_range, data_after_handshake};
            }

            std::string_view response_str{reinterpret_cast<const char*>(response_buffer.data()), bytes_read};

            auto [parse_ec, response] = http::detail::parse_response_partial(response_str);
            if (parse_ec) {
              co_await self->async_finalize_session({}, return_as_deferred_tuple());
              co_return {parse_ec, std::move(response)};
            }

            // Perform upgrade challenge with server handshake response
            auto challenge_ec = self->client_handshaker_.validate_server_handshake(response, handshake->sec_websocket_key);
            if (challenge_ec) {
              co_await self->async_finalize_session({}, return_as_deferred_tuple());
              co_return {challenge_ec, std::move(response)};
            }

            self->set_connection_state(state::open);
            co_return {std::error_code{}, std::move(response)};
          },
          strand_),
        bound_token,
        this,
        std::move(parsed_url),
        std::move(headers));
    }

    template <typename CompletionToken>
    auto async_connect(urls::url url, http::headers headers, CompletionToken&& token) {
      return async_connect(std::expected<urls::url, std::error_code>{std::move(url)},
        std::move(headers),
        std::forward<CompletionToken>(token));
    }

    template <typename CompletionToken>
    auto async_connect(std::string_view url, http::headers headers, CompletionToken&& token) {
      return async_connect(urls::url::parse(url), std::move(headers), std::forward<CompletionToken>(token));
    }

    template <typename CompletionToken>
    auto async_connect(urls::url url, CompletionToken&& token) {
      return async_connect(std::move(url), http::headers{}, std::forward<CompletionToken>(token));
    }

    template <typename CompletionToken>
    auto async_connect(std::expected<urls::url, std::error_code> parsed_url, CompletionToken&& token) {
      return async_connect(std::move(parsed_url), http::headers{}, std::forward<CompletionToken>(token));
    }

    template <typename CompletionToken>
    auto async_connect(std::string_view url, CompletionToken&& token) {
      return async_connect(url, http::headers{}, std::forward<CompletionToken>(token));
    }

    // Caller must ensure that given buffer remains valid until the operation is completed
    template <typename CompletionToken>
    auto async_send_text(std::string_view text, CompletionToken&& token) {
      auto bound_token = asio::bind_allocator(aero::detail::aligned_allocator<>{}, std::forward<CompletionToken>(token));

      return asio::async_initiate<decltype(bound_token), void(std::error_code)>(
        asio::co_composed<void(std::error_code)>(
          [](auto, basic_connection* self, std::string_view text) -> void {
            if (!self->is_current_state(state::open) || self->is_close_received()) {
              co_return protocol_error::connection_closed;
            }

            auto frame = self->client_frame_builder_.build_text_frame(text);
            if (!frame) {
              co_return frame.error();
            }

            co_return co_await self->async_write_bytes(*frame, return_as_deferred_tuple());
          },
          strand_),
        bound_token,
        this,
        text);
    }

    // Caller must ensure that given buffer remains valid until the operation is completed
    template <typename CompletionToken>
    auto async_send_binary(std::span<const std::byte> data, CompletionToken&& token) {
      auto bound_token = asio::bind_allocator(aero::detail::aligned_allocator<>{}, std::forward<CompletionToken>(token));

      return asio::async_initiate<decltype(bound_token), void(std::error_code)>(
        asio::co_composed<void(std::error_code)>(
          [](auto, basic_connection* self, std::span<const std::byte> data) -> void {
            if (!self->is_current_state(state::open) || self->is_close_received()) {
              co_return protocol_error::connection_closed;
            }

            auto frame = self->client_frame_builder_.build_binary_frame(data);
            if (!frame) {
              co_return frame.error();
            }

            co_return co_await self->async_write_bytes(*frame, return_as_deferred_tuple());
          },
          strand_),
        bound_token,
        this,
        data);
    }

    template <typename CompletionToken>
    auto async_ping(std::string_view text, CompletionToken&& token) {
      std::span text_bytes(reinterpret_cast<const std::byte*>(text.data()), text.size());
      return async_ping(text_bytes, std::forward<CompletionToken>(token));
    }

    template <typename CompletionToken>
    auto async_ping(CompletionToken&& token) {
      return async_ping(null_bytes, std::forward<CompletionToken>(token));
    }

    template <typename CompletionToken>
    auto async_ping(std::span<const std::byte> data, CompletionToken&& token) {
      auto bound_token = asio::bind_allocator(aero::detail::aligned_allocator<>{}, std::forward<CompletionToken>(token));

      return asio::async_initiate<decltype(bound_token), void(std::error_code)>(
        asio::co_composed<void(std::error_code)>(
          [](auto, basic_connection* self, std::span<const std::byte> data) -> void {
            if (!self->is_current_state(state::open) || self->is_close_received()) {
              co_return protocol_error::connection_closed;
            }

            auto frame = self->client_frame_builder_.build_ping_frame(data);
            if (!frame) {
              co_return frame.error();
            }

            co_return co_await self->async_write_bytes(*frame, return_as_deferred_tuple());
          },
          strand_),
        bound_token,
        this,
        data);
    }

    template <typename CompletionToken>
    auto async_pong(std::span<const std::byte> data, CompletionToken&& token) {
      auto bound_token = asio::bind_allocator(aero::detail::aligned_allocator<>{}, std::forward<CompletionToken>(token));

      return asio::async_initiate<decltype(bound_token), void(std::error_code)>(
        asio::co_composed<void(std::error_code)>(
          [](auto, basic_connection* self, std::span<const std::byte> data) -> void {
            if (!self->is_current_state(state::open, state::closing) || self->is_close_received()) {
              co_return protocol_error::connection_closed;
            }

            auto frame = self->client_frame_builder_.build_pong_frame(data);
            if (!frame) {
              co_return frame.error();
            }

            co_return co_await self->async_write_bytes(*frame, return_as_deferred_tuple());
          },
          strand_),
        bound_token,
        this,
        data);
    }

    template <typename CompletionToken>
    auto async_pong(std::string_view text, CompletionToken&& token) {
      std::span text_bytes(reinterpret_cast<const std::byte*>(text.data()), text.size());
      return async_pong(text_bytes, std::forward<CompletionToken>(token));
    }

    template <typename CompletionToken>
    auto async_pong(CompletionToken&& token) {
      return async_pong(null_bytes, std::forward<CompletionToken>(token));
    }

    template <typename CompletionToken>
    auto async_close(websocket::close_code code, std::string_view reason, CompletionToken&& token) {
      auto bound_token = asio::bind_allocator(aero::detail::aligned_allocator<>{}, std::forward<CompletionToken>(token));

      return asio::async_initiate<decltype(bound_token), void(std::error_code)>(
        asio::co_composed<void(std::error_code)>(
          [](auto, basic_connection* self, websocket::close_code close_code, std::string_view close_reason) -> void {
            if (is_close_code_server_only(close_code)) {
              co_return protocol_error::close_code_server_only;
            }

            if (self->is_current_state(state::closed)) {
              co_return protocol_error::connection_closed;
            }

            if (self->is_current_state(state::closing)) {
              co_return protocol_error::already_closing;
            }

            self->close_result_ec_.reset();
            self->set_connection_state(state::closing);

            auto [send_close_ec] = co_await self->async_send_close(close_code, close_reason, return_as_deferred_tuple());
            if (send_close_ec) {
              co_return co_await self->async_finalize_session(send_close_ec, return_as_deferred_tuple());
            }

            aero::deadline close_deadline{default_close_timeout};
            self->consume_data_received_in_handshake_if_present();

            // Avoid "lost wakeup" problem
            if (auto result = self->take_close_result()) {
              co_return *result;
            }

            // An async_read is in progress, we wait for it to receive
            // the close frame response from peer (or to timeout)
            if (self->is_read_loop_active()) {
              self->close_timer_.expires_after(close_deadline.remaining());

              // Avoid "lost wakeup" problem
              if (auto result = self->take_close_result()) {
                co_return *result;
              }

              auto [wait_ec] = co_await self->close_timer_.async_wait(asio::as_tuple(asio::deferred));
              if (!wait_ec) {
                // Peer close response was not received, timed out
                co_return co_await self->async_finalize_session(asio::error::timed_out, return_as_deferred_tuple());
              }

              // Close response was received in read loop and it woke up our 'close_timer_'
              if (is_canceled(wait_ec)) {
                if (auto result = self->take_close_result()) {
                  co_return *result;
                }
                co_return std::error_code{};
              }

              // Unexpected error from timer
              co_return co_await self->async_finalize_session(wait_ec, return_as_deferred_tuple());
            }

            // If no read loop active, then start our own read-loop
            // until close frame is received or timeout expires
            for (;;) {
              if (close_deadline.expired()) {
                co_return co_await self->async_finalize_session(asio::error::timed_out, return_as_deferred_tuple());
              }

              auto [read_ec, message] =
                co_await self->async_read(asio::cancel_after(close_deadline.remaining(), return_as_deferred_tuple()));

              if (read_ec) {
                // Read was canceled due to timeout expiring
                if (is_canceled(read_ec) && close_deadline.expired()) {
                  co_return co_await self->async_finalize_session(asio::error::timed_out, return_as_deferred_tuple());
                }

                // Consider any other error as transport fail
                co_return co_await self->async_finalize_session(read_ec, return_as_deferred_tuple());
              }

              // Received peer's close response - handshake complete
              if (message.is_close()) {
                co_return co_await self->async_finalize_session(std::error_code{}, return_as_deferred_tuple());
              }
            }
          },
          strand_),
        bound_token,
        this,
        code,
        reason);
    }

    template <typename CompletionToken>
    auto async_close(websocket::close_code code, CompletionToken&& token) {
      return async_close(code, "", std::forward<CompletionToken>(token));
    }

    // Tear down transport without performing close handshake
    template <typename CompletionToken>
    auto async_force_close(CompletionToken&& token) {
      return async_finalize_session(std::error_code{}, std::forward<CompletionToken>(token));
    }

    template <typename CompletionToken>
    auto async_read(CompletionToken&& token) {
      auto bound_token = asio::bind_allocator(aero::detail::aligned_allocator<>{}, std::forward<CompletionToken>(token));

      return asio::async_initiate<decltype(bound_token), void(std::error_code, websocket::message)>(
        asio::co_composed<void(std::error_code, websocket::message)>(
          [](auto, basic_connection* self) -> void {
            if (self->read_buffer_.capacity() == 0) {
              self->read_buffer_.resize(self->max_read_buffer_size_);
            }

            // Prevent concurrent async_read operations (one read at a time)
            if (self->is_read_loop_active()) {
              co_return {protocol_error::already_reading, websocket::message{}};
            }

            self->set_read_loop_active_flag(true);
            auto _ = aero::finally([self] { self->set_read_loop_active_flag(false); });

            for (;;) {
              // If a close handshake is in progress or connection is closed, stop reading
              if (self->is_current_state(state::closed) || self->is_close_received()) {
                co_return {protocol_error::connection_closed, websocket::message{}};
              }

              self->consume_data_received_in_handshake_if_present();

              // Deliver next assembled message if available
              if (auto message = self->message_reader_.poll()) {
                if (message->is_control()) {
                  // Auto-respond to control frames
                  auto [response_ec] = co_await self->async_respond_to_control_message(*message, return_as_deferred_tuple());
                  if (response_ec) {
                    co_return {response_ec, websocket::message{}};
                  }

                  // Received a close frame - send close reply (if not sent) and finalize session
                  // Also wakes up any pending async_close waiting on a timer
                  if (message->is_close()) {
                    auto [final_ec] = co_await self->async_finalize_session(std::error_code{}, return_as_deferred_tuple());
                    if (final_ec) {
                      co_return {final_ec, websocket::message{}};
                    }
                    // Return the close message
                    co_return {std::error_code{}, std::move(*message)};
                  }

                  co_return {std::error_code{}, std::move(*message)};
                }

                if (self->is_current_state(state::closing) || self->is_close_sent()) {
                  continue;
                }

                // Return any non-control or handled control message to the caller
                co_return {std::error_code{}, std::move(*message)};
              }

              // If a deferred error was stored (e.g. from a previous consume), handle it now
              if (self->deferred_read_ec_) {
                auto deferred_read_ec = *self->deferred_read_ec_;
                self->deferred_read_ec_.reset();
                if (is_fatal_websocket_error(deferred_read_ec)) {
                  co_await self->async_fail_connection(deferred_read_ec, return_as_deferred_tuple());
                }
                co_return {deferred_read_ec, websocket::message{}};
              }

              auto [read_ec, bytes_read] =
                co_await self->transport_->async_read_some(self->get_mutable_read_buffer(), return_as_deferred_tuple());
              if (read_ec) {
                // The read was canceled (possibly by async_close or timeout)
                if (is_canceled(read_ec)) {
                  co_return {read_ec, websocket::message{}};
                }

                // Unexpected transport error - fail the WebSocket connection (RFC 6455 7.2.1)
                auto [final_ec] = co_await self->async_finalize_session(read_ec, return_as_deferred_tuple());

                // Forward unexpected transport errors to a caller for better
                // understanding of why the transport was closed, who initiated
                // the closure, whether it was broken unexpectedly, etc.
                co_return {final_ec, websocket::message{}};
              }

              // Consume incoming bytes into WebSocket frames/messages
              auto consume_ec = self->message_reader_.consume(std::span{self->read_buffer_}.first(bytes_read));
              if (consume_ec && !self->deferred_read_ec_) {
                // Store the first error to report after delivering any remaining message
                self->deferred_read_ec_ = consume_ec;
              }

              // Loop continues to check for assembled messages or handle errors
            }
          },
          strand_),
        bound_token,
        this);
    }

    std::tuple<std::error_code, http::response> connect(std::expected<urls::url, std::error_code> parsed_url,
      http::headers headers = {}) {
      if (!is_current_state(state::closed)) {
        return {protocol_error::connection_not_closed, {}};
      }

      if (!parsed_url) {
        return {parsed_url.error(), {}};
      }

      const urls::url& url = *parsed_url;
      if (auto ec = validate_websocket_url(url); ec) {
        return {ec, http::response{}};
      }

      auto port = websocket::get_port_for_scheme(url);
      if (!port) {
        return {port.error(), http::response{}};
      }

      if (auto ec = construct_transport(aero::striequal(url.scheme(), "wss")); ec) {
        return {ec, {}};
      }

      reset_connection_state(state::connecting);

      auto connect_ec = transport_->connect(std::string{url.host()}, *port);
      if (connect_ec) {
        std::ignore = finalize_session();
        return {connect_ec, {}};
      }

      // Build bodyless HTTP websocket upgrade request
      auto handshake = client_handshaker_.build_request(url, std::move(headers));
      if (!handshake) {
        std::ignore = finalize_session();
        return {handshake.error(), {}};
      }

      std::error_code write_ec;
      asio::write(*transport_, asio::buffer(handshake->bytes()), write_ec);
      if (write_ec) {
        std::ignore = finalize_session();
        return {write_ec, {}};
      }

      std::error_code read_ec;
      std::vector<std::byte> response_buffer;

      // Read server response until "\r\n\r\n"
      std::size_t bytes_read =
        asio::read_until(*transport_, asio::dynamic_buffer(response_buffer), http::detail::double_crlf, read_ec);
      if (read_ec) {
        std::ignore = finalize_session();
        return {read_ec, {}};
      }

      // https://www.boost.org/doc/libs/1_43_0/doc/html/boost_asio/reference/read_until.html
      // "After a successful async_read_until operation, the streambuf
      // may contain additional data beyond the delimiter"
      const bool buffer_has_data_after_delimiter = response_buffer.size() > bytes_read;
      if (buffer_has_data_after_delimiter) {
        auto data_after_handshake = std::span{response_buffer}.subspan(bytes_read);
        data_received_in_handshake_ = std::vector{std::from_range, data_after_handshake};
      }

      std::string_view response_str{reinterpret_cast<const char*>(response_buffer.data()), bytes_read};

      auto [parse_ec, response] = http::detail::parse_response_partial(response_str);
      if (parse_ec) {
        std::ignore = finalize_session();
        return {parse_ec, std::move(response)};
      }

      // Perform upgrade challenge with server handshake response
      auto challenge_ec = client_handshaker_.validate_server_handshake(response, handshake->sec_websocket_key);
      if (challenge_ec) {
        std::ignore = finalize_session();
        return {challenge_ec, std::move(response)};
      }

      set_connection_state(state::open);
      return {std::error_code{}, std::move(response)};
    }

    std::tuple<std::error_code, http::response> connect(urls::url url, http::headers headers = {}) {
      return connect(std::expected<urls::url, std::error_code>{std::move(url)}, std::move(headers));
    }

    std::tuple<std::error_code, http::response> connect(std::string_view url_string, http::headers headers = {}) {
      return connect(urls::url::parse(url_string), std::move(headers));
    }

    std::error_code send_text(std::string_view text) {
      if (!is_current_state(state::open) || is_close_received()) {
        return protocol_error::connection_closed;
      }

      auto frame = client_frame_builder_.build_text_frame(text);
      if (!frame) {
        return frame.error();
      }

      return write_bytes(*frame);
    }

    std::error_code send_binary(std::span<const std::byte> data) {
      if (!is_current_state(state::open) || is_close_received()) {
        return protocol_error::connection_closed;
      }

      auto frame = client_frame_builder_.build_binary_frame(data);
      if (!frame) {
        return frame.error();
      }

      return write_bytes(*frame);
    }

    std::error_code ping(std::span<const std::byte> data) {
      if (!is_current_state(state::open) || is_close_received()) {
        return protocol_error::connection_closed;
      }

      auto frame = client_frame_builder_.build_ping_frame(data);
      if (!frame) {
        return frame.error();
      }

      return write_bytes(*frame);
    }

    std::error_code ping(std::string_view text) {
      return ping(std::as_bytes(std::span{text}));
    }

    std::error_code ping() {
      return ping(null_bytes);
    }

    std::error_code pong(std::span<const std::byte> data) {
      if (!is_current_state(state::open, state::closing) || is_close_received()) {
        return protocol_error::connection_closed;
      }

      auto frame = client_frame_builder_.build_pong_frame(data);
      if (!frame) {
        return frame.error();
      }

      return write_bytes(*frame);
    }

    std::error_code pong(std::string_view text) {
      return pong(std::as_bytes(std::span{text}));
    }

    std::error_code pong() {
      return pong(null_bytes);
    }

    std::error_code close(websocket::close_code code) {
      return synchronize_awaitable<std::error_code>(async_close(code, return_as_awaitable_tuple()));
    }

    std::error_code close(websocket::close_code code, std::string_view reason) {
      return synchronize_awaitable<std::error_code>(async_close(code, reason, return_as_awaitable_tuple()));
      return {};
    }

    std::error_code force_close() {
      return synchronize_awaitable<std::error_code>(async_force_close(return_as_awaitable_tuple()));
    }

    std::expected<websocket::message, std::error_code> read() {
      if (read_buffer_.capacity() == 0) {
        read_buffer_.resize(max_read_buffer_size_);
      }

      // Prevent multiple read operations (one read at a time)
      if (is_read_loop_active()) {
        return std::unexpected(protocol_error::already_reading);
      }

      set_read_loop_active_flag(true);
      aero::final_action on_finish{[this] { set_read_loop_active_flag(false); }};

      for (;;) {
        // If a close handshake is in progress or connection is closed, stop reading
        if (is_current_state(state::closed) || is_close_received()) {
          return std::unexpected(protocol_error::connection_closed);
        }

        consume_data_received_in_handshake_if_present();

        // Deliver next assembled message if available
        if (auto message = message_reader_.poll()) {
          if (message->is_control()) {
            // Auto-respond to control frames
            std::error_code response_ec = respond_to_control_message(*message);
            if (response_ec) {
              return std::unexpected(response_ec);
            }

            // Received a close frame - send close reply (if not sent) and finalize session
            if (message->is_close()) {
              std::error_code final_ec = finalize_session();
              if (final_ec) {
                return std::unexpected(final_ec);
              }
              // Return the close message
              return *message;
            }

            return *message;
          }

          if (is_current_state(state::closing) || is_close_sent()) {
            continue;
          }

          // Return any non-control or handled control message to the caller
          return *message;
        }

        // If a deferred error was stored (e.g. from a previous consume), handle it now
        if (deferred_read_ec_) {
          auto deferred_read_ec = *deferred_read_ec_;
          deferred_read_ec_.reset();
          if (is_fatal_websocket_error(deferred_read_ec)) {
            fail_connection(deferred_read_ec);
          }
          return std::unexpected(deferred_read_ec);
        }

        std::error_code read_ec;
        std::size_t bytes_read = transport_->read_some(get_mutable_read_buffer(), read_ec);
        if (read_ec) {
          // Unexpected transport error - fail the WebSocket connection (RFC 6455 7.2.1)
          std::error_code final_ec = finalize_session(read_ec);

          // Forward unexpected transport errors to a caller for better
          // understanding of why the transport was closed, who initiated the
          // closure, whether it was broken unexpectedly, etc.
          return std::unexpected(final_ec);
        }

        // Consume incoming bytes into WebSocket frames/messages
        auto consume_ec = message_reader_.consume(std::span{read_buffer_}.first(bytes_read));
        if (consume_ec && !deferred_read_ec_) {
          // Store the first error to report after delivering any remaining message
          deferred_read_ec_ = consume_ec;
        }

        // Loop continues to check for assembled messages or handle errors
      }
    }

    [[nodiscard]] bool is_open_for_writing() const noexcept {
      return is_current_state(state::open) && !is_close_received();
    }

    [[nodiscard]] bool is_connecting() const noexcept {
      return is_current_state(state::connecting);
    }

    [[nodiscard]] bool is_closed() const noexcept {
      return is_current_state(state::closed);
    }

    [[nodiscard]] bool is_closing() const noexcept {
      return is_current_state(state::closing);
    }

    [[nodiscard]] bool is_transport_secure() const noexcept {
      return using_secure_transport_.load(std::memory_order::acquire);
    }

    [[nodiscard]] executor_type get_executor() const noexcept {
      return strand_;
    }

    [[nodiscard]] asio::strand<executor_type> get_strand() const noexcept {
      return strand_;
    }

   private:
    static asio::as_tuple_t<asio::deferred_t> return_as_deferred_tuple() {
      return asio::as_tuple(asio::deferred);
    }

    static asio::as_tuple_t<asio::use_awaitable_t<>> return_as_awaitable_tuple() {
      return asio::as_tuple(asio::use_awaitable);
    }

    static bool is_canceled(std::error_code ec) {
      return ec == asio::error::operation_aborted;
    }

    static bool is_fatal_websocket_error(std::error_code ec) {
      return websocket::is_invalid_payload(ec) || websocket::is_protocol_violation(ec);
    }

    static websocket::close_code close_code_for_error(std::error_code ec) {
      if (websocket::is_invalid_payload(ec)) {
        return close_code::invalid_payload;
      }
      if (ec == websocket::protocol_error::message_too_big) {
        return close_code::message_too_big;
      }
      if (websocket::is_protocol_violation(ec)) {
        return close_code::protocol_error;
      }

      return {};
    }

    static std::error_code validate_websocket_url(const urls::url& url) {
      if (!url.has_authority() || url.host().empty()) {
        return urls::url_error::authority_invalid;
      }

      bool is_using_secure_transport = aero::striequal(url.scheme(), "wss");
      if (!aero::striequal(url.scheme(), "ws") && !is_using_secure_transport) {
        return urls::url_error::scheme_invalid;
      }

      return {};
    }

    void consume_data_received_in_handshake_if_present() {
      if (!data_received_in_handshake_) {
        return;
      }

      auto consume_ec = message_reader_.consume(*data_received_in_handshake_);
      data_received_in_handshake_.reset();
      if (consume_ec && !deferred_read_ec_) {
        deferred_read_ec_ = consume_ec;
      }
    }

    template <typename CompletionToken>
    auto async_write_bytes(std::span<const std::byte> frame, CompletionToken&& token) {
      auto bound_token = asio::bind_allocator(aero::detail::aligned_allocator<>{}, std::forward<CompletionToken>(token));

      return asio::async_initiate<decltype(bound_token), void(std::error_code)>(
        asio::co_composed<void(std::error_code)>(
          [](auto state, basic_connection* self, std::span<const std::byte> frame) -> void {
            auto [write_ec, bytes_written] = co_await self->transport_->async_write(frame, return_as_deferred_tuple());
            if (!write_ec) {
              co_return std::error_code{};
            }

            // Filtering cancellation errors only partially, because after
            // cancelling composed asio::async_write, the transport stream
            // may contain partially written WebSocket frame, in cases when
            // one or more calls to .async_write_some() were done before the
            // cancellation.
            //
            // Imagine sending binary with 1MB payload and timeout of 50ms,
            // surely, some data, including WebSocket frame header, will be
            // sent to a peer. Peer received WebSocket frame header, expects
            // payload with size equal to 1MB, but the caller is cancelling
            // composed write operation after sending a couple of chunks to a
            // peer. This means that the connection is broken and all that
            // aero can do is a force shutdown
            //
            // But! There is also an edge-case in current aero transport
            // architecture: write queue. Everything said above applies only
            // to the in-flight write cancellation. If write was enqueued and
            // hasn't yet started, we can safely cancel the operation. In
            // cases when in-flight write was cancelled, transport closes a
            // socket, so we can decide whether caller has canceled in-flight
            // operation or not by simply checking if socket is still open.
            if (is_canceled(write_ec) && self->transport_->is_open()) {
              co_return {write_ec};
            }

            // Allow co_await-ing async_finalize_session even if the current
            // co_composed received cancellation
            state.reset_cancellation_state(asio::disable_cancellation());

            // RFC 6455, Section 7.2.1:
            // If at any point the underlying transport layer connection is
            // unexpectedly lost, the client MUST _Fail the WebSocket Connection_.
            co_return co_await self->async_finalize_session(write_ec, return_as_deferred_tuple());
          },
          strand_),
        bound_token,
        this,
        frame);
    }

    std::error_code write_bytes(std::span<const std::byte> frame) {
      std::error_code write_ec;
      std::size_t bytes_written = asio::write(*transport_, asio::buffer(frame), write_ec);
      if (!write_ec) {
        return std::error_code{};
      }

      // RFC 6455, Section 7.2.1:
      // If at any point the underlying transport layer connection is
      // unexpectedly lost, the client MUST _Fail the WebSocket Connection_.
      return finalize_session(write_ec);
    }

    template <typename CompletionToken>
    auto async_send_close(websocket::close_code code, std::optional<std::string_view> reason, CompletionToken&& token) {
      auto bound_token = asio::bind_allocator(aero::detail::aligned_allocator<>{}, std::forward<CompletionToken>(token));

      return asio::async_initiate<decltype(bound_token), void(std::error_code)>(
        asio::co_composed<void(std::error_code)>(
          [](auto, basic_connection* self, close_code code, std::optional<std::string_view> reason) -> void {
            if (self->is_close_sent()) {
              co_return std::error_code{};
            }

            auto close_frame = self->client_frame_builder_.build_close_frame(code, reason);
            if (!close_frame) {
              co_return close_frame.error();
            }

            auto [write_ec] = co_await self->async_write_bytes(*close_frame, return_as_deferred_tuple());
            if (write_ec) {
              co_return write_ec;
            }

            self->set_close_sent_flag(true);
            co_return std::error_code{};
          },
          strand_),
        bound_token,
        this,
        code,
        std::move(reason));
    }

    std::error_code send_close(websocket::close_code code, std::optional<std::string_view> reason = std::nullopt) {
      if (is_close_sent()) {
        return std::error_code{};
      }

      auto close_frame = client_frame_builder_.build_close_frame(code, reason);
      if (!close_frame) {
        return close_frame.error();
      }

      std::error_code write_ec = write_bytes(*close_frame);
      if (write_ec) {
        return write_ec;
      }

      set_close_sent_flag(true);
      return std::error_code{};
    }

    // Fail fast websocket termination path. Use when we detected a fatal
    // websocket violation (protocol error, invalid payload etc.) and must
    // actively fail the connection. Sends a close frame with the appropriate
    // error code to the peer, drains a little, then force-shutdowns the
    // transport and resets all internal state
    template <typename CompletionToken>
    auto async_fail_connection(std::error_code fatal_ec, CompletionToken&& token) {
      auto bound_token = asio::bind_allocator(aero::detail::aligned_allocator<>{}, std::forward<CompletionToken>(token));

      return asio::async_initiate<decltype(bound_token), void()>(
        asio::co_composed<void()>(
          [](auto, basic_connection* self, std::error_code fatal_ec) -> void {
            if (!self->is_current_state(state::closed)) {
              self->set_connection_state(state::closing);
            }

            auto [send_close_ec] =
              co_await self->async_send_close(close_code_for_error(fatal_ec), std::nullopt, return_as_deferred_tuple());
            if (!send_close_ec) {
              // RFC 6455, Section 7.1.1:
              // An endpoint SHOULD use a method that cleanly closes the TCP
              // connection, as well as the TLS session, if applicable,
              // discarding any trailing bytes that may have been received.
              aero::deadline drain_deadline{transport_drain_deadline};

              while (!drain_deadline.expired()) {
                auto [read_ec, bytes_read] = co_await self->transport_->async_read_some(self->get_mutable_read_buffer(),
                  asio::cancel_after(drain_deadline.remaining(), return_as_deferred_tuple()));
                if (read_ec) {
                  break;
                }
              }
            }

            self->set_close_received_flag(true);
            self->deferred_read_ec_.reset();
            self->data_received_in_handshake_.reset();
            self->message_reader_.reset();

            // We don't care whether force-shutdown returned an error or not
            std::ignore = co_await self->async_finalize_session(fatal_ec, return_as_deferred_tuple());

            co_return {};
          },
          strand_),
        bound_token,
        this,
        fatal_ec);
    }

    void fail_connection(std::error_code fatal_ec) {
      if (!is_current_state(state::closed)) {
        set_connection_state(state::closing);
      }

      std::error_code send_close_ec = send_close(close_code_for_error(fatal_ec));

      // We will not perform transport drainage on the sync path. This is
      // merely "desirable" behavior during closure, however, it could
      // potentially cause blocking and various TLS-related issues if we
      // decide to use a non-blocking approach

      set_close_received_flag(true);
      deferred_read_ec_.reset();
      data_received_in_handshake_.reset();
      message_reader_.reset();

      // We don't care whether force-shutdown returned an error or not
      std::ignore = finalize_session(fatal_ec);
    }

    // Graceful connection finalization path.
    // Use after a normal close handshake (or any non-fatal error) to move the
    // client to 'closed' state. Stops all further reads/writes, shutdowns the
    // transport, and resets internal state. This does not initiate a protocol
    // failure, only finalizes/cleans up
    template <typename CompletionToken>
    auto async_finalize_session(std::error_code final_ec, CompletionToken&& token) {
      auto bound_token = asio::bind_allocator(aero::detail::aligned_allocator<>{}, std::forward<CompletionToken>(token));

      return asio::async_initiate<decltype(bound_token), void(std::error_code)>(
        asio::co_composed<void(std::error_code)>(
          [](auto state, basic_connection* self, std::error_code final_ec) -> void {
            // Disable cancellation via the coroutine associated cancellation
            // slot for cleanup path. Imagine a situation where we enter
            // 'async_finalize_session' and cancellation has already been
            // signalled through the associated cancellation slot. It means that
            // when we call 'transport_.async_shutdown', the shutdown operation
            // could be cancelled immediately and return 'operation_aborted',
            // potentially leaving the underlying websocket transport still open
            state.reset_cancellation_state(asio::disable_cancellation());

            if (self->is_current_state(state::closed)) {
              self->signal_close_completion(final_ec);
              co_return final_ec;
            }

            self->reset_connection_state(state::closed);

            auto [shutdown_ec] = co_await self->transport_->async_shutdown(return_as_deferred_tuple());

            std::error_code result_ec = final_ec ? final_ec : shutdown_ec;

            // Wake up any pending close operation
            self->signal_close_completion(result_ec);
            co_return result_ec;
          },
          strand_),
        bound_token,
        this,
        final_ec);
    }

    std::error_code finalize_session(std::error_code final_ec = {}) {
      if (is_current_state(state::closed)) {
        return final_ec;
      }

      reset_connection_state(state::closed);

      std::error_code shutdown_ec = transport_->shutdown();
      return final_ec ? final_ec : shutdown_ec;
    }

    template <typename CompletionToken>
    auto async_respond_to_control_message(const websocket::message& message, CompletionToken&& token) {
      auto bound_token = asio::bind_allocator(aero::detail::aligned_allocator<>{}, std::forward<CompletionToken>(token));

      return asio::async_initiate<decltype(bound_token), void(std::error_code)>(
        asio::co_composed<void(std::error_code)>(
          [](auto, basic_connection* self, const websocket::message& message) -> void {
            if (message.is_ping()) {
              co_return co_await self->async_pong(message.payload, return_as_deferred_tuple());
            }

            if (message.is_close()) {
              self->set_close_received_flag(true);

              auto reply_close_code = message.close_code().value_or(close_code::normal);
              co_return co_await self->async_send_close(reply_close_code, message.close_reason(), return_as_deferred_tuple());
            }

            co_return std::error_code{};
          },
          strand_),
        bound_token,
        this,
        message);
    }

    std::error_code respond_to_control_message(const websocket::message& message) {
      if (message.is_ping()) {
        return pong(message.payload);
      }

      if (message.is_close()) {
        set_close_received_flag(true);

        auto reply_close_code = message.close_code().value_or(close_code::normal);
        return send_close(reply_close_code, message.close_reason());
      }

      return std::error_code{};
    }

    template <typename... States>
      requires((std::same_as<States, websocket::state>) && ...)
    [[nodiscard]] bool is_current_state(States... state) const noexcept
      requires(sizeof...(state) > 0)
    {
      auto current_state = state_.load(std::memory_order::acquire);
      return ((current_state == state) || ...);
    }

    [[nodiscard]] bool is_close_received() const noexcept {
      return close_received_.load(std::memory_order::acquire);
    }

    [[nodiscard]] bool is_close_sent() const noexcept {
      return close_sent_.load(std::memory_order::acquire);
    }

    [[nodiscard]] bool is_read_loop_active() const noexcept {
      return read_loop_active_.load(std::memory_order::acquire);
    }

    void set_close_received_flag(bool value) noexcept {
      close_received_.store(value, std::memory_order::release);
    }

    void set_close_sent_flag(bool value) noexcept {
      close_sent_.store(value, std::memory_order::release);
    }

    void set_read_loop_active_flag(bool value) noexcept {
      read_loop_active_.store(value, std::memory_order::release);
    }

    void set_secure_transport_flag(bool value) noexcept {
      using_secure_transport_.store(value, std::memory_order::release);
    }

    void set_connection_state(websocket::state state) noexcept {
      state_.store(state, std::memory_order::release);
    }

    void reset_connection_state(websocket::state state) noexcept {
      if (state == websocket::state::closed) {
        set_secure_transport_flag(false);
      }

      set_connection_state(state);
      set_close_received_flag(false);
      set_close_sent_flag(false);
      deferred_read_ec_.reset();
      data_received_in_handshake_.reset();
      message_reader_.reset();
    }

    std::error_code construct_transport(bool is_secure) {
      if (!is_secure) {
        transport_.emplace(strand_);
        set_secure_transport_flag(false);
        return {};
      }

#if AERO_USE_TLS
      asio::ssl::context* ctx = ssl_ctx_;
      if (ctx == nullptr) {
        auto& default_ctx = tls::detail::default_context();
        if (!default_ctx) {
          return default_ctx.error();
        }
        ctx = std::addressof(default_ctx->context());
      }

      transport_.emplace(strand_, *ctx);
      set_secure_transport_flag(true);
      return {};
#else
      return tls::backend_error::unavailable;
#endif
    }

    void signal_close_completion(std::error_code result_ec) {
      close_result_ec_ = result_ec;
      close_timer_.cancel();
    }

    std::optional<std::error_code> take_close_result() {
      if (!close_result_ec_) {
        return std::nullopt;
      }
      auto result = *close_result_ec_;
      close_result_ec_.reset();
      return result;
    }

    asio::mutable_buffer get_mutable_read_buffer() {
      return {read_buffer_.data(), read_buffer_.size()};
    }

    template <typename ResultT, typename F>
      requires(not std::same_as<ResultT, std::error_code>)
    std::tuple<std::error_code, ResultT> synchronize_awaitable(F&& awaitable) {
      if (strand_.running_in_this_thread()) {
        return {aero::basic_error::deadlock_would_occur, {}};
      }

      try {
        return asio::co_spawn(strand_, std::forward<F>(awaitable), asio::use_future).get();
      } catch (const std::system_error& e) {
        return {e.code(), {}};
      } catch (const std::future_error& e) {
        return {e.code(), {}};
      } catch (...) {
        return {make_error_code(std::errc::io_error), {}};
      }
    }

    template <typename ResultT, typename F>
      requires(std::same_as<ResultT, std::error_code>)
    std::error_code synchronize_awaitable(F&& awaitable) {
      if (strand_.running_in_this_thread()) {
        return aero::basic_error::deadlock_would_occur;
      }

      try {
        auto [ec] = asio::co_spawn(strand_, std::forward<F>(awaitable), asio::use_future).get();
        return ec;
      } catch (const std::system_error& e) {
        return e.code();
      } catch (const std::future_error& e) {
        return e.code();
      } catch (...) {
        return make_error_code(std::errc::io_error);
      }
    }

    asio::strand<executor_type> strand_;
    websocket::detail::client_frame_builder<> client_frame_builder_;
    websocket::detail::message_reader message_reader_;
    websocket::client_handshaker client_handshaker_;

    // Store this as a separate variable to avoid allocating
    // extra memory for the connections that will never read
    std::size_t max_read_buffer_size_{websocket::default_read_buffer_size};

    // Lazy-allocated on the first read call
    std::vector<std::byte> read_buffer_;
    std::atomic<bool> using_secure_transport_{false};
    std::optional<transport_type> transport_;
#if AERO_USE_TLS
    asio::ssl::context* ssl_ctx_{nullptr};
#endif

    asio::steady_timer close_timer_{strand_};
    std::optional<std::error_code> close_result_ec_;
    std::optional<std::vector<std::byte>> data_received_in_handshake_;
    std::atomic<websocket::state> state_{state::closed};
    std::atomic<bool> close_sent_{false};
    std::atomic<bool> close_received_{false};
    std::atomic<bool> read_loop_active_{false};
    std::optional<std::error_code> deferred_read_ec_;
  };

} // namespace aero::websocket
