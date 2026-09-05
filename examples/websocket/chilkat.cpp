#include <print>
#include <string_view>

#include "aero/websocket/client.hpp"

namespace websocket = aero::websocket;

void print_error(std::string_view message, const std::error_code& ec) {
  std::println("{}: {} ({} - {})", message, ec.message(), ec.value(), ec.category().name());
}

void print_message(const websocket::message& message) {
  switch (message.kind) {
  case websocket::message_kind::text:
    std::println("Received text: {}", message.text());
    break;
  case websocket::message_kind::binary:
    std::println("Received binary of size {}", message.payload.size());
    break;
  case websocket::message_kind::pong:
    if (message.has_payload()) {
      // Assume that the ping content was valid UTF-8 text, so we expect the same payload to be echoed
      std::println("Received pong with payload: {}", message.text());
    } else {
      std::println("Received pong");
    }
    break;
  case websocket::message_kind::close:
    std::println("Received close with code {} and reason {}",
      message.close_code().value_or(websocket::close_code::no_status_received),
      message.close_reason().value_or("no reason"));
    break;
  default:
    std::println("Received message of kind {}", message.kind);
  }
}

int main() {
  websocket::client client;

  auto [connect_ec, handshake_resp] = client.connect("ws://websockets.chilkat.io/wsChilkatEcho.ashx");
  if (connect_ec) {
    print_error("Connect to echo server failed", connect_ec);
    return 1;
  }

  auto text_ec = client.send_text("hello from aero client");
  if (text_ec) {
    print_error("Text send failed", text_ec);
    return 1;
  }

  auto read_result = client.read();
  if (!read_result.has_value()) {
    print_error("Read failed", read_result.error());
    return 1;
  }

  print_message(read_result.value());

  std::println("Initiating connection close");

  auto close_ec = client.close(websocket::close_code::normal, "aero client is leaving, byye!");
  if (close_ec) {
    print_error("Closing connection failed", close_ec);
    return 1;
  }

  std::println("Connection succesfully closed. Done.");
}
