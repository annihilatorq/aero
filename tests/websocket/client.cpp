#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <expected>
#include <functional>
#include <future>
#include <latch>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <asio/as_tuple.hpp>
#include <asio/bind_cancellation_slot.hpp>
#include <asio/bind_executor.hpp>
#include <asio/buffer.hpp>
#include <asio/cancellation_signal.hpp>
#include <asio/error.hpp>
#include <asio/io_context.hpp>
#include <asio/post.hpp>
#include <asio/thread_pool.hpp>
#include <asio/use_future.hpp>

#include <ut/ut.hpp>

#include "aero/http/detail/line_endings.hpp"
#include "aero/http/error.hpp"
#include "aero/http/headers.hpp"
#include "aero/http/status.hpp"
#include "aero/http/status_line.hpp"
#include "aero/urls/url.hpp"
#include "aero/util/deadline.hpp"
#include "aero/util/final_action.hpp"
#include "aero/websocket/client.hpp"
#include "aero/websocket/connection_options.hpp"
#include "aero/websocket/detail/accept_challenge.hpp"
#include "aero/websocket/detail/client_frame_builder.hpp"
#include "aero/websocket/detail/opcode.hpp"
#include "aero/websocket/error.hpp"

#include "tcp_server.hpp"
#include "websocket/test_helpers.hpp"

using namespace ut;

namespace http = aero::http;
namespace websocket = aero::websocket;

using aero::tests::websocket::serialize_close_payload;
using aero::tests::websocket::serialize_unmasked_frame;
using aero::tests::websocket::to_bytes;
using aero::tests::websocket::to_string;
using aero::tests::websocket::unmask_payload;
using aero::websocket::detail::masking_key;
using aero::websocket::detail::opcode;
using namespace std::chrono_literals;

namespace {

  std::string extract_sec_websocket_key(std::string_view raw_request) {
    auto request_line_end = raw_request.find(http::detail::crlf);
    if (request_line_end == std::string_view::npos) {
      throw std::runtime_error{"websocket request line terminator is missing"};
    }

    auto parsed_headers = http::headers::parse(raw_request.substr(request_line_end + http::detail::crlf.size()));
    if (!parsed_headers.has_value()) {
      throw std::system_error{parsed_headers.error()};
    }

    auto key = parsed_headers->first_value("sec-websocket-key");
    if (!key.has_value()) {
      throw std::runtime_error{"sec-websocket-key header is missing"};
    }

    return std::string{*key};
  }

  std::string make_websocket_switching_response(std::string_view request_str, std::string_view extra_headers = {}) {
    auto accept = websocket::detail::compute_sec_websocket_accept(extract_sec_websocket_key(request_str));

    std::string response;
    response.append("HTTP/1.1 101 Switching Protocols\r\n");
    response.append("Upgrade: websocket\r\n");
    response.append("Connection: Upgrade\r\n");
    response.append("Sec-WebSocket-Accept: ").append(accept).append(http::detail::crlf);
    response.append(extra_headers);
    response.append(http::detail::crlf);

    return response;
  }

  std::vector<std::byte> read_masked_frame_payload(connection& conn, opcode expected_opcode) {
    auto header = to_bytes(conn.read_bytes(2));
    auto first_byte = std::to_integer<std::uint8_t>(header[0]);
    auto second_byte = std::to_integer<std::uint8_t>(header[1]);

    if ((first_byte & 0x0FU) != static_cast<std::uint8_t>(expected_opcode)) {
      throw std::runtime_error{"unexpected frame opcode"};
    }
    if ((second_byte & 0x80U) == 0U) {
      throw std::runtime_error{"expected a masked frame"};
    }

    auto payload_length = static_cast<std::size_t>(second_byte & 0x7FU);
    auto key_bytes = to_bytes(conn.read_bytes(4));
    masking_key key{key_bytes[0], key_bytes[1], key_bytes[2], key_bytes[3]};

    return unmask_payload(to_bytes(conn.read_bytes(payload_length)), key);
  }

  std::uint16_t read_masked_close_code(connection& conn) {
    auto payload = read_masked_frame_payload(conn, opcode::close);
    if (payload.size() < 2U) {
      throw std::runtime_error{"expected a close frame carrying a close code"};
    }

    return static_cast<std::uint16_t>(
      (std::to_integer<std::uint8_t>(payload[0]) << 8U) | std::to_integer<std::uint8_t>(payload[1]));
  }

} // namespace

int main() {
  tcp_server server;
  std::string port_str = std::to_string(server.port());
  std::string url_str = "ws://127.0.0.1:" + port_str + "/socket";

  suite websocket_client = [&] {
    "connect completes a valid handshake and returns the parsed 101 response"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request, "X-Test: yes\r\n"));
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);

      expect(not static_cast<bool>(connect_ec));
      expect(response.status_code() == http::status::switching_protocols);
      expect(response.headers.contains_token("upgrade", "websocket"));
      expect(response.headers.first_value("x-test") == "yes");
    };

    "connect sends a valid RFC 6455 upgrade request"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      std::string raw_request;
      server.on_accept([&](std::shared_ptr<connection> conn) {
        raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request));
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);

      expect(not static_cast<bool>(connect_ec));

      auto request_line_end = raw_request.find(http::detail::crlf);
      expect(raw_request.substr(0, request_line_end) == "GET /socket HTTP/1.1");

      auto request_headers = http::headers::parse(raw_request.substr(request_line_end + http::detail::crlf.size()));
      require(request_headers.has_value());
      expect(request_headers->first_value("host") == "127.0.0.1:" + port_str);
      expect(request_headers->contains_token("upgrade", "websocket"));
      expect(request_headers->contains_token("connection", "upgrade"));
      expect(request_headers->first_value("sec-websocket-version") == "13");
      expect(request_headers->first_value("sec-websocket-key").value_or("").size() == 24U);
    };

    "connect reports the status line parse error for a malformed response status line"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        conn->write_response("TP/1.1 101 Switching Protocols\r\n"
                             "Upgrade: websocket\r\n"
                             "Connection: Upgrade\r\n"
                             "\r\n");
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);

      expect(static_cast<bool>(connect_ec));
      expect(connect_ec == http::status_line::parse("TP/1.1 101 Switching Protocols").error());
    };

    "connect reports the header parse error for a malformed response header field"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        conn->write_response("HTTP/1.1 101 Switching Protocols\r\n"
                             "Upgrade websocket\r\n"
                             "\r\n");
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);

      expect(static_cast<bool>(connect_ec));
      expect(connect_ec == http::header_error::field_invalid);
    };

    "connect reports accept_challenge_failed but still returns the parsed response for a wrong accept key"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        conn->write_response("HTTP/1.1 101 Switching Protocols\r\n"
                             "Upgrade: websocket\r\n"
                             "Connection: Upgrade\r\n"
                             "Sec-WebSocket-Accept: definitely-not-the-accept-challenge\r\n"
                             "X-Trace: parsed\r\n"
                             "\r\n");
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);

      expect(connect_ec == websocket::handshake_error::accept_challenge_failed);
      expect(response.status_code() == http::status::switching_protocols);
      expect(response.headers.contains_token("upgrade", "websocket"));
      expect(response.headers.first_value("x-trace") == "parsed");
    };

    "connect reports status_code_invalid when the response is not 101"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        conn->write_response("HTTP/1.1 200 OK\r\n"
                             "Content-Length: 0\r\n"
                             "\r\n");
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);

      expect(connect_ec == websocket::handshake_error::status_code_invalid);
    };

    "connect reports upgrade_header_invalid when the response omits the upgrade header"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        conn->write_response("HTTP/1.1 101 Switching Protocols\r\n"
                             "Connection: Upgrade\r\n"
                             "Sec-WebSocket-Accept: placeholder\r\n"
                             "\r\n");
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);

      expect(connect_ec == websocket::handshake_error::upgrade_header_invalid);
    };

    "connect reports connection_header_invalid when the response omits the connection header"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        conn->write_response("HTTP/1.1 101 Switching Protocols\r\n"
                             "Upgrade: websocket\r\n"
                             "Sec-WebSocket-Accept: placeholder\r\n"
                             "\r\n");
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);

      expect(connect_ec == websocket::handshake_error::connection_header_invalid);
    };

    "connect reports accept_header_invalid when the response omits the accept header"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        conn->write_response("HTTP/1.1 101 Switching Protocols\r\n"
                             "Upgrade: websocket\r\n"
                             "Connection: Upgrade\r\n"
                             "\r\n");
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);

      expect(connect_ec == websocket::handshake_error::accept_header_invalid);
    };

    "connect rejects a non-websocket url scheme before connecting"_test = [&] {
      websocket::client client;
      auto [connect_ec, response] = client.connect("http://127.0.0.1/socket");

      expect(connect_ec == aero::urls::url_error::scheme_invalid);
    };

    "connect rejects a url without a host before connecting"_test = [&] {
      websocket::client client;
      auto [connect_ec, response] = client.connect("ws:///socket");

      expect(connect_ec == aero::urls::url_error::authority_invalid);
    };

    "connect accepts a urls::url value"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request));
      });

      auto parsed_url = aero::urls::url::parse(url_str);
      require(parsed_url.has_value());

      websocket::client client;
      auto [connect_ec, response] = client.connect(std::move(*parsed_url));

      expect(not static_cast<bool>(connect_ec)) << "connect with a urls::url failed: " << connect_ec.message();
      expect(response.status_code() == http::status::switching_protocols);
    };

    "connect accepts the std::expected result of urls::url::parse"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request));
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(aero::urls::url::parse(url_str));

      expect(not static_cast<bool>(connect_ec)) << "connect with a parse result failed: " << connect_ec.message();
      expect(response.status_code() == http::status::switching_protocols);
    };

    "connect reports connection_refused when nothing is listening and a retry succeeds"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      std::uint16_t unused_port = 0;
      {
        asio::io_context probe_context;
        tcp::acceptor probe{probe_context, tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0)};
        unused_port = probe.local_endpoint().port();
      }

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request));
      });

      websocket::client client;
      auto [refused_ec, refused_response] = client.connect("ws://127.0.0.1:" + std::to_string(unused_port) + "/socket");

      expect(refused_ec == asio::error::connection_refused)
        << "connect to a port with no listener should be refused, got: " << refused_ec.message();
      expect(client.is_closed()) << "failed connect must return the connection to the closed state";

      auto [connect_ec, response] = client.connect(url_str);
      expect(not static_cast<bool>(connect_ec)) << "connect after a refused attempt failed: " << connect_ec.message();
      expect(response.status_code() == http::status::switching_protocols);
    };

    "read returns the message the server sent in the same packet as the handshake response"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        auto response = make_websocket_switching_response(raw_request);
        response += to_string(serialize_unmasked_frame(opcode::text, true, to_bytes("hello")));
        conn->write_response(response);
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);
      expect(not static_cast<bool>(connect_ec));

      auto message = client.read();
      expect(message.has_value()) << "frame bytes received together with the handshake must reach the reader";
      if (not message.has_value()) {
        return;
      }

      expect(message->is_text());
      expect(to_string(message->payload) == "hello");
    };

    "ping sends a masked ping frame carrying the given bytes"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      std::vector<std::byte> received_payload;
      std::latch ping_received{1};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request));
        received_payload = read_masked_frame_payload(*conn, opcode::ping);
        ping_received.count_down();
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);
      expect(not static_cast<bool>(connect_ec));

      auto ping_ec = client.ping(to_bytes("keepalive"));
      expect(not static_cast<bool>(ping_ec)) << "ping failed: " << ping_ec.message();

      ping_received.wait();
      expect(to_string(received_payload) == "keepalive") << "ping payload must reach the peer unchanged";
    };

    "ping with bytes returns connection_closed before connect"_test = [&] {
      websocket::client client;
      auto ping_ec = client.ping(to_bytes("keepalive"));

      expect(ping_ec == websocket::protocol_error::connection_closed);
    };

    "send_text sends a masked text frame carrying the given text"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      std::vector<std::byte> received_payload;
      std::latch frame_received{1};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request));
        received_payload = read_masked_frame_payload(*conn, opcode::text);
        frame_received.count_down();
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);
      expect(not static_cast<bool>(connect_ec));

      auto send_ec = client.send_text("hello");
      expect(not static_cast<bool>(send_ec)) << "send_text failed: " << send_ec.message();

      frame_received.wait();
      expect(to_string(received_payload) == "hello") << "text payload must reach the peer unchanged";
    };

    "send_binary sends a masked binary frame carrying the given bytes"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      std::vector<std::byte> received_payload;
      std::latch frame_received{1};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request));
        received_payload = read_masked_frame_payload(*conn, opcode::binary);
        frame_received.count_down();
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);
      expect(not static_cast<bool>(connect_ec));

      std::vector<std::byte> payload{std::byte{0x00}, std::byte{0x01}, std::byte{0xFF}};
      auto send_ec = client.send_binary(payload);
      expect(not static_cast<bool>(send_ec)) << "send_binary failed: " << send_ec.message();

      frame_received.wait();
      expect(received_payload == payload) << "binary payload must reach the peer unchanged";
    };

    "send_text returns connection_closed before connect"_test = [&] {
      websocket::client client;
      auto send_ec = client.send_text("hello");

      expect(send_ec == websocket::protocol_error::connection_closed);
    };

    "read returns message_too_big and fails the connection with close code 1009 when a message exceeds max_message_size"_test =
      [&] {
        aero::final_action cleanup{[&] { server.close_last_conn(); }};

        constexpr std::size_t max_message_size = 16;
        std::uint16_t received_close_code = 0;
        std::latch close_received{1};

        server.on_accept([&](std::shared_ptr<connection> conn) {
          auto raw_request = conn->read_request();
          conn->write_response(make_websocket_switching_response(raw_request));

          std::vector<std::byte> oversized(max_message_size + 1);
          conn->write_response(to_string(serialize_unmasked_frame(opcode::binary, true, oversized)));

          received_close_code = read_masked_close_code(*conn);
          close_received.count_down();
        });

        websocket::client client{websocket::connection_options{.max_message_size = max_message_size}};
        auto [connect_ec, response] = client.connect(url_str);
        expect(not static_cast<bool>(connect_ec));

        auto message = client.read();
        close_received.wait();

        expect(!message.has_value() && message.error() == websocket::protocol_error::message_too_big);
        expect(client.is_closed()) << "an oversized message must fail the connection, not leave it open for reuse";
        expect(received_close_code == 1009U)
          << "close frame should carry close code 1009 (message too big), got: " << received_close_code;
      };

    "send during connect returns connection_closed"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      std::latch request_received{1};
      std::latch response_sent{1};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        request_received.count_down();
        response_sent.wait();
        conn->write_response(make_websocket_switching_response(raw_request));
      });

      websocket::client client;
      auto connect_future = client.async_connect(url_str, asio::as_tuple(asio::use_future));
      request_received.wait();

      auto send_ec = client.send_text("hello");
      response_sent.count_down();
      auto [connect_ec, response] = connect_future.get();

      expect(send_ec == websocket::protocol_error::connection_closed)
        << "send while the handshake is still in progress must be refused, got: " << send_ec.message();
      expect(not static_cast<bool>(connect_ec));
    };

    "connect returns connection_not_closed while the connection is open"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request));
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);
      expect(not static_cast<bool>(connect_ec));

      auto [reconnect_ec, reconnect_response] = client.connect(url_str);
      expect(reconnect_ec == websocket::protocol_error::connection_not_closed)
        << "second connect must be refused without touching the open connection, got: " << reconnect_ec.message();
      expect(client.is_open_for_writing()) << "refused connect must leave the open connection usable";
    };

    "connect cancelled while waiting for the handshake response still finalizes the session"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      std::latch request_reached_peer{1};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        std::ignore = conn->read_request();
        request_reached_peer.count_down();
      });

      websocket::client client;
      asio::cancellation_signal cancel_signal;
      auto connect_future =
        client.async_connect(url_str, asio::bind_cancellation_slot(cancel_signal.slot(), asio::as_tuple(asio::use_future)));

      request_reached_peer.wait();
      asio::post(client.get_executor(), [&] { cancel_signal.emit(asio::cancellation_type::terminal); });
      auto [connect_ec, response] = connect_future.get();

      expect(connect_ec == asio::error::operation_aborted)
        << "cancelled connect should complete with operation_aborted, got: " << connect_ec.message();
      expect(client.is_closed()) << "cancelled connect must finalize the session, not leave it in the connecting state";
    };

    "connect cancelled after TCP connected but before the handshake is sent still finalizes the session"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      std::latch peer_accepted{1};

      server.on_accept([&](std::shared_ptr<connection>) { peer_accepted.count_down(); });

      asio::thread_pool pool{2};
      websocket::client client{pool.get_executor()};
      asio::cancellation_signal cancel_signal;

      // The connect completion has to reach the coroutine only after
      // cancellation is signalled. Holding the strand keeps that completion
      // queued while the other pool thread finishes the TCP connect
      asio::post(client.get_executor(), [&] {
        peer_accepted.wait();
        std::this_thread::sleep_for(200ms);
        cancel_signal.emit(asio::cancellation_type::terminal);
      });

      // TODO: Remove executor binding after https://github.com/annihilatorq/aero/issues/91 is fixed
      auto connect_future = client.async_connect(url_str,
        asio::bind_cancellation_slot(cancel_signal.slot(),
          asio::bind_executor(client.get_executor(), asio::as_tuple(asio::use_future))));
      auto [connect_ec, response] = connect_future.get();

      expect(connect_ec == asio::error::operation_aborted)
        << "cancelled connect should complete with operation_aborted, got: " << connect_ec.message();
      expect(client.is_closed()) << "connect that observed cancellation after TCP connected must finalize the session";
    };

    "cancelled in-flight send fails the connection"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      std::latch frame_reached_peer{1};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request));
        std::ignore = conn->read_bytes(2);
        frame_reached_peer.count_down();
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);
      expect(not static_cast<bool>(connect_ec));

      std::vector<std::byte> unflushable(64ULL * 1024 * 1024);
      asio::cancellation_signal cancel_signal;
      auto send_future = client.async_send_binary(unflushable,
        asio::bind_cancellation_slot(cancel_signal.slot(), asio::as_tuple(asio::use_future)));

      frame_reached_peer.wait();
      asio::post(client.get_executor(), [&] { cancel_signal.emit(asio::cancellation_type::terminal); });
      auto [send_ec] = send_future.get();

      expect(send_ec == asio::error::operation_aborted)
        << "cancelled send should complete with operation_aborted, got: " << send_ec.message();
      expect(not client.is_open_for_writing()) << "connection with a partially written frame must not accept further writes";
      expect(client.is_closed()) << "cancelled in-flight send must fail the connection, not leave it open";
    };

    "read cancelled after the peer already dropped TCP still finalizes the session"_test = [&] {
      std::latch peer_may_close{1};
      std::latch peer_closed{1};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request));

        peer_may_close.wait();
        conn->close();
        peer_closed.count_down();
      });

      asio::thread_pool pool{2};
      websocket::client client{pool.get_executor()};
      auto [connect_ec, response] = client.connect(url_str);
      expect(not static_cast<bool>(connect_ec));

      // TODO: Remove executor binding after https://github.com/annihilatorq/aero/issues/91 is fixed
      asio::cancellation_signal cancel_signal;
      auto read_future = client.async_read(asio::bind_cancellation_slot(cancel_signal.slot(),
        asio::bind_executor(client.get_executor(), asio::as_tuple(asio::use_future))));

      // The read completes on the strand, so holding the strand keeps that
      // completion queued while the other pool thread turns the peer's close
      // into EOF. Cancelling here therefore no longer aborts the read
      asio::post(client.get_executor(), [&] {
        peer_may_close.count_down();
        peer_closed.wait();
        std::this_thread::sleep_for(200ms);
        cancel_signal.emit(asio::cancellation_type::terminal);
      });

      auto [read_ec, message] = read_future.get();

      expect(read_ec == asio::error::eof) << "read that observed cancellation after EOF should report EOF, got: "
                                          << read_ec.message();
      expect(client.is_closed()) << "read that observed cancellation after EOF must finalize the session";
    };

    "transport drain in async_fail_connection shares a single deadline across multiple reads"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      std::latch small_packets_sent{1};
      std::latch text_frame_built{1};

      bool text_frame_was_built{false};
      websocket::detail::client_frame_builder frame_builder;

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto request = conn->read_request();

        conn->write_response(make_websocket_switching_response(request));

        auto text_frame = frame_builder.build_text_frame("lol");
        text_frame_was_built = text_frame.has_value();
        text_frame_built.count_down();

        if (not text_frame_was_built) {
          return;
        }

        conn->write_response(to_string(*text_frame));

        aero::deadline deadline{3s};

        while (not deadline.expired()) {
          try {
            conn->write_response("lool");
          } catch (...) {
            break;
          }
          std::this_thread::sleep_for(10ms);
        }

        small_packets_sent.count_down();
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);
      expect(not static_cast<bool>(connect_ec));

      text_frame_built.wait();
      expect(text_frame_was_built) << "server failed to build text frame";

      std::error_code read_ec;
      client.async_read(asio::redirect_error(asio::use_future, read_ec)).wait();

      // Frame that came from server was built with client_frame_builder,
      // so it will be masked, and client MUST refuse masked frames
      expect(read_ec == websocket::protocol_error::masked_frame_from_server);

      small_packets_sent.wait();

      expect(client.is_closed()) << "connection should be closed after 3 seconds of server sending small packets while client "
                                    "was draining its transport";
    };

    "close sends a masked close frame and returns once the peer's close reply arrives"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      std::uint16_t received_close_code = 0;

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request));

        received_close_code = read_masked_close_code(*conn);
        auto close_reply = serialize_close_payload(websocket::close_code::normal, {});
        conn->write_response(to_string(serialize_unmasked_frame(opcode::close, true, close_reply)));
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);
      expect(not static_cast<bool>(connect_ec));

      auto close_ec = client.close(websocket::close_code::normal);
      expect(not static_cast<bool>(close_ec)) << "close failed: " << close_ec.message();
      expect(client.is_closed()) << "connection must be closed after the close handshake";
      expect(received_close_code == 1000U) << "close frame should carry close code 1000, got: " << received_close_code;
    };

    "close returns eof and closes the connection when the peer drops TCP instead of replying"_test = [&] {
      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request));

        std::ignore = read_masked_close_code(*conn);
        conn->close();
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);
      expect(not static_cast<bool>(connect_ec));

      auto close_ec = client.close(websocket::close_code::normal);
      expect(close_ec == asio::error::eof) << "got: " << close_ec.message();
      expect(client.is_closed()) << "lost transport must leave the connection closed";
    };

    "close cancelled while waiting for the peer's close reply still finalizes the session"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      std::latch close_frame_reached_peer{1};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request));

        std::ignore = read_masked_close_code(*conn);
        close_frame_reached_peer.count_down();
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);
      expect(not static_cast<bool>(connect_ec));

      asio::cancellation_signal cancel_signal;
      auto close_future = client.async_close(websocket::close_code::normal,
        asio::bind_cancellation_slot(cancel_signal.slot(), asio::as_tuple(asio::use_future)));

      close_frame_reached_peer.wait();
      asio::post(client.get_executor(), [&] { cancel_signal.emit(asio::cancellation_type::terminal); });
      auto [close_ec] = close_future.get();

      expect(close_ec == asio::error::operation_aborted)
        << "cancelled close should complete with operation_aborted, got: " << close_ec.message();
      expect(client.is_closed()) << "cancelled close must finalize the session, not leave it in the closing state";
      expect(not client.is_open_for_writing()) << "cancelled close must shut the transport down";
    };

    "close cancelled while a concurrent read waits for the peer's close reply still finalizes the session"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      std::latch close_frame_reached_peer{1};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request));

        std::ignore = read_masked_close_code(*conn);
        close_frame_reached_peer.count_down();
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);
      expect(not static_cast<bool>(connect_ec));

      auto read_future = client.async_read(asio::as_tuple(asio::use_future));

      asio::cancellation_signal cancel_signal;
      auto close_future = client.async_close(websocket::close_code::normal,
        asio::bind_cancellation_slot(cancel_signal.slot(), asio::as_tuple(asio::use_future)));

      close_frame_reached_peer.wait();
      asio::post(client.get_executor(), [&] { cancel_signal.emit(asio::cancellation_type::terminal); });
      auto [close_ec] = close_future.get();

      expect(close_ec == asio::error::operation_aborted)
        << "cancelled close must not report success without the peer's close reply, got: " << close_ec.message();
      expect(client.is_closed()) << "cancelled close must finalize the session even while another read is waiting";

      auto [read_ec, message] = read_future.get();
      expect(static_cast<bool>(read_ec)) << "concurrent read must be woken by the finalized session";
    };

    "close with reason sends the code and reason in the close frame"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      std::vector<std::byte> received_payload;

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request));

        received_payload = read_masked_frame_payload(*conn, opcode::close);
        auto close_reply = serialize_close_payload(websocket::close_code::normal, {});
        conn->write_response(to_string(serialize_unmasked_frame(opcode::close, true, close_reply)));
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);
      expect(not static_cast<bool>(connect_ec));

      auto close_ec = client.close(websocket::close_code::going_away, "bye");
      expect(not static_cast<bool>(close_ec)) << "close failed: " << close_ec.message();
      expect(client.is_closed());

      auto expected_payload = serialize_close_payload(websocket::close_code::going_away, to_bytes("bye"));
      expect(received_payload == expected_payload)
        << "close payload must be the code followed by the reason, got: " << to_string(received_payload);
    };

    "close with a reason longer than 123 bytes returns control_frame_payload_too_big and closes the connection"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request));
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);
      expect(not static_cast<bool>(connect_ec));

      auto close_ec = client.close(websocket::close_code::normal, std::string(124, 'x'));
      expect(close_ec == websocket::protocol_error::control_frame_payload_too_big) << "got: " << close_ec.message();
      expect(client.is_closed()) << "close that could not send its frame must still tear the connection down";
    };

    "close with a reason that is not valid utf-8 returns close_reason_invalid_utf8 and closes the connection"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request));
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);
      expect(not static_cast<bool>(connect_ec));

      auto close_ec = client.close(websocket::close_code::normal, "\xC3\x28");
      expect(close_ec == websocket::protocol_error::close_reason_invalid_utf8) << "got: " << close_ec.message();
      expect(client.is_closed()) << "close that could not send its frame must still tear the connection down";
    };

    "close returns connection_closed before connect"_test = [&] {
      websocket::client client;
      auto close_ec = client.close(websocket::close_code::normal);

      expect(close_ec == websocket::protocol_error::connection_closed);
    };

    "force_close closes the transport without sending a close frame"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      std::error_code peer_read_ec;
      std::latch peer_finished{1};

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto raw_request = conn->read_request();
        conn->write_response(make_websocket_switching_response(raw_request));

        std::array<char, 1> sink{};
        conn->socket.read_some(asio::buffer(sink), peer_read_ec);
        peer_finished.count_down();
      });

      websocket::client client;
      auto [connect_ec, response] = client.connect(url_str);
      expect(not static_cast<bool>(connect_ec));

      auto force_close_ec = client.force_close();
      expect(not static_cast<bool>(force_close_ec)) << "force_close failed: " << force_close_ec.message();
      expect(client.is_closed());

      peer_finished.wait();
      expect(peer_read_ec == asio::error::eof) << "peer must see eof without a close frame, got: " << peer_read_ec.message();
    };

    "force_close before connect succeeds"_test = [&] {
      websocket::client client;

      expect(client.force_close() == std::error_code{});
      expect(client.is_closed());
    };

    "test server handled all requests without throwing"_test = [&] {
      expect(server.exception() == nullptr);
    };
  };
}
