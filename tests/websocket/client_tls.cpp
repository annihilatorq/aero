#include <array>
#include <cstddef>
#include <exception>
#include <future>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#include <asio/as_tuple.hpp>
#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/address.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read.hpp>
#include <asio/read_until.hpp>
#include <asio/ssl.hpp>
#include <asio/use_future.hpp>
#include <asio/write.hpp>

#include <ut/ut.hpp>

#include "aero/http/detail/line_endings.hpp"
#include "aero/http/headers.hpp"
#include "aero/websocket/client.hpp"
#include "aero/websocket/close_code.hpp"
#include "aero/websocket/detail/accept_challenge.hpp"
#include "aero/websocket/detail/opcode.hpp"

#include "common/tls_context.hpp"
#include "websocket/test_helpers.hpp"

using namespace ut;

namespace http = aero::http;
namespace websocket = aero::websocket;

using tcp = asio::ip::tcp;
using aero::tests::websocket::serialize_close_payload;
using aero::tests::websocket::serialize_unmasked_frame;
using aero::websocket::detail::opcode;

namespace {

  std::string make_websocket_switching_response(std::string_view raw_request) {
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

    auto accept = websocket::detail::compute_sec_websocket_accept(std::string{*key});

    std::string response;
    response.append("HTTP/1.1 101 Switching Protocols\r\n");
    response.append("Upgrade: websocket\r\n");
    response.append("Connection: Upgrade\r\n");
    response.append("Sec-WebSocket-Accept: ").append(accept).append(http::detail::crlf);
    response.append(http::detail::crlf);

    return response;
  }

  void read_masked_close_frame(asio::ssl::stream<tcp::socket>& stream) {
    std::array<std::byte, 2> header{};
    asio::read(stream, asio::buffer(header));

    if (std::to_integer<unsigned>(header[0] & std::byte{0x0F}) != 0x08U) {
      throw std::runtime_error{"expected a close frame"};
    }
    if (std::to_integer<unsigned>(header[1] & std::byte{0x80}) == 0U) {
      throw std::runtime_error{"expected a masked close frame"};
    }

    auto payload_length = std::to_integer<std::size_t>(header[1] & std::byte{0x7F});
    std::vector<std::byte> mask_and_payload(4U + payload_length);
    asio::read(stream, asio::buffer(mask_and_payload));
  }

  void send_close_response_then_do_tcp_fin(asio::ssl::stream<tcp::socket>& stream) {
    std::string request;
    asio::read_until(stream, asio::dynamic_buffer(request), http::detail::double_crlf);

    auto response = make_websocket_switching_response(request);
    asio::write(stream, asio::buffer(response));

    read_masked_close_frame(stream);

    auto close_frame =
      serialize_unmasked_frame(opcode::close, true, serialize_close_payload(websocket::close_code::normal, {}));
    asio::write(stream, asio::buffer(close_frame.data(), close_frame.size()));

    // TCP FIN without a TLS close_notify
    stream.next_layer().shutdown(asio::socket_base::shutdown_send);

    for (;;) {
      std::array<char, 256> sink{};
      std::error_code read_ec;
      stream.next_layer().read_some(asio::buffer(sink), read_ec);
      if (read_ec) {
        break;
      }
    }

    std::error_code close_ec;
    static_cast<void>(stream.next_layer().close(close_ec));
  }

} // namespace

int main() {
  asio::io_context io_context;
  tcp::acceptor acceptor{io_context, tcp::endpoint{asio::ip::make_address("127.0.0.1"), 0}};
  std::string port_str = std::to_string(acceptor.local_endpoint().port());
  std::string url_str = "wss://127.0.0.1:" + port_str + "/socket";

  auto server_ctx = aero::tests::make_tls_server_context();
  std::exception_ptr server_failure;
  std::thread server_thread{[&] {
    try {
      for (int connection = 0; connection < 2; ++connection) {
        asio::ssl::stream<tcp::socket> stream{io_context, server_ctx};
        acceptor.accept(stream.next_layer());
        stream.handshake(asio::ssl::stream_base::server);

        send_close_response_then_do_tcp_fin(stream);
      }
    } catch (...) {
      server_failure = std::current_exception();
    }
  }};

  suite websocket_client_tls = [&] {
    "close succeeds when the peer closes TCP without sending TLS close_notify"_test = [&] {
      auto client_ctx = aero::tests::make_tls_client_context();
      websocket::client client{client_ctx};
      auto [connect_ec, response] = client.connect(url_str);
      expect(not static_cast<bool>(connect_ec)) << "connect failed: " << connect_ec.message();

      auto close_ec = client.close(websocket::close_code::normal);
      expect(not static_cast<bool>(close_ec)) << "close handshake reported: " << close_ec.message();
      expect(client.is_closed());
    };

    "async_close succeeds when the peer closes TCP without sending TLS close_notify"_test = [&] {
      auto client_ctx = aero::tests::make_tls_client_context();
      websocket::client client{client_ctx};
      auto connect_future = client.async_connect(url_str, asio::as_tuple(asio::use_future));
      auto [connect_ec, response] = connect_future.get();
      expect(not static_cast<bool>(connect_ec)) << "async_connect failed: " << connect_ec.message();

      auto close_future = client.async_close(websocket::close_code::normal, asio::as_tuple(asio::use_future));
      auto [close_ec] = close_future.get();
      expect(not static_cast<bool>(close_ec)) << "close handshake reported: " << close_ec.message();
      expect(client.is_closed());
    };

    "test server handled both connections without throwing"_test = [&] {
      server_thread.join();
      expect(server_failure == nullptr);
    };
  };
}
