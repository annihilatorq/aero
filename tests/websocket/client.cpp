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
#include <asio/cancellation_signal.hpp>
#include <asio/error.hpp>
#include <asio/post.hpp>
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

using aero::tests::websocket::serialize_unmasked_frame;
using aero::tests::websocket::to_bytes;
using aero::tests::websocket::to_string;
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

  std::uint16_t read_masked_close_code(connection& conn) {
    auto header = conn.read_bytes(2);
    auto first_byte = static_cast<std::uint8_t>(header[0]);
    auto second_byte = static_cast<std::uint8_t>(header[1]);

    if ((first_byte & 0x0FU) != 0x08U) {
      throw std::runtime_error{"expected a close frame"};
    }

    auto payload_length = static_cast<std::size_t>(second_byte & 0x7FU);
    if ((second_byte & 0x80U) == 0U || payload_length < 2U) {
      throw std::runtime_error{"expected a masked close frame carrying a close code"};
    }

    auto mask = conn.read_bytes(4);
    auto payload = conn.read_bytes(payload_length);
    auto unmasked = [&](std::size_t index) {
      return static_cast<std::uint8_t>(static_cast<std::uint8_t>(payload[index]) ^ static_cast<std::uint8_t>(mask[index % 4U]));
    };

    return static_cast<std::uint16_t>((unmasked(0) << 8U) | unmasked(1));
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

    "read returns message_too_big and fails the connection with close code 1009 when a message exceeds max_message_size"_test =
      [&] {
        aero::final_action cleanup{[&] { server.close_last_conn(); }};

        constexpr std::size_t max_message_size = 16;
        std::uint16_t received_close_code = 0;

        server.on_accept([&](std::shared_ptr<connection> conn) {
          auto raw_request = conn->read_request();
          conn->write_response(make_websocket_switching_response(raw_request));

          std::vector<std::byte> oversized(max_message_size + 1);
          conn->write_response(to_string(serialize_unmasked_frame(opcode::binary, true, oversized)));

          received_close_code = read_masked_close_code(*conn);
          conn->close();
        });

        websocket::client client{websocket::connection_options{.max_message_size = max_message_size}};
        auto [connect_ec, response] = client.connect(url_str);
        expect(not static_cast<bool>(connect_ec));

        auto message = client.read();
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

    "transport drain in async_fail_connection shares a single deadline across multiple reads"_test = [&] {
      aero::final_action cleanup{[&] { server.close_last_conn(); }};

      std::latch small_packets_sent{1};

      websocket::detail::client_frame_builder frame_builder;

      server.on_accept([&](std::shared_ptr<connection> conn) {
        auto request = conn->read_request();

        conn->write_response(make_websocket_switching_response(request));

        auto text_frame = frame_builder.build_text_frame("lol");
        expect(text_frame.has_value());
        if (not text_frame.has_value()) {
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

      client.async_read([&](std::error_code ec, auto) {
        // Frame that came from server was built with client_frame_builder,
        // so it will be masked, and client MUST refuse masked frames
        expect(ec == websocket::protocol_error::masked_frame_from_server);
      });

      small_packets_sent.wait();

      expect(client.is_closed()) << "connection should be closed after 3 seconds of server sending small packets while client "
                                    "was draining its transport";
    };

    "test server handled all requests without throwing"_test = [&] {
      expect(server.exception() == nullptr);
    };
  };
}
