#include <charconv>
#include <cstdint>
#include <expected>
#include <format>
#include <print>
#include <string>
#include <string_view>
#include <system_error>

#include <asio/any_io_executor.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/io_context.hpp>
#include <asio/use_awaitable.hpp>

#include "aero/websocket/client.hpp"
#include "aero/websocket/message.hpp"

namespace websocket = aero::websocket;

std::expected<std::uint32_t, std::error_code> parse_u32(std::string_view text) {
  std::uint32_t value = 0;
  auto parse_result = std::from_chars(text.data(), text.data() + text.size(), value);
  if (parse_result.ec != std::errc{} || parse_result.ptr != text.data() + text.size()) {
    return std::unexpected(std::make_error_code(std::errc::invalid_argument));
  }
  return value;
}

std::error_code run_autobahn_client(std::string base_url, std::string agent) {
  {
    websocket::client client;

    auto [connect_ec, handshake_resp] = client.connect(std::format("{}/getCaseCount", base_url));
    if (connect_ec) {
      return connect_ec;
    }

    auto read_result = client.read();
    if (!read_result) {
      return read_result.error();
    }

    const auto& received = read_result.value();
    if (!received.is_text()) {
      return std::make_error_code(std::errc::protocol_error);
    }

    auto case_count_result = parse_u32(received.text());
    if (!case_count_result) {
      return case_count_result.error();
    }

    std::uint32_t case_count = *case_count_result;
    std::println("Autobahn case count: {}", case_count);

    for (std::uint32_t case_id = 1; case_id <= case_count; ++case_id) {
      websocket::client case_client;

      auto [connect_ec, handshake_resp] =
        case_client.connect(std::format("{}/runCase?case={}&agent={}", base_url, case_id, agent));
      if (connect_ec) {
        std::println("Case {} connect failed: {}", case_id, connect_ec.message());
        continue;
      }

      for (;;) {
        auto case_message_result = case_client.read();
        if (!case_message_result) {
          std::println("Case {} read failed: {}", case_id, case_message_result.error().message());
          break;
        }

        const auto& received = *case_message_result;

        if (received.is_text()) {
          auto send_ec = case_client.send_text(received.text());
          if (send_ec) {
            std::println("Case {} send text failed: {}", case_id, send_ec.message());
            break;
          }
          continue;
        }

        if (received.is_binary()) {
          auto send_ec = case_client.send_binary(received.bytes());
          if (send_ec) {
            std::println("Case {} send binary failed: {}", case_id, send_ec.message());
            break;
          }
          continue;
        }

        if (received.kind == websocket::message_kind::close) {
          break;
        }
      }
    }
  }

  {
    websocket::client client;
    auto [connect_ec, handshake_resp] = client.connect(std::format("{}/updateReports?agent={}", base_url, agent));
    if (connect_ec) {
      return connect_ec;
    }

    for (;;) {
      auto read_result = client.read();
      if (!read_result) {
        break;
      }
      if (read_result->is_close()) {
        break;
      }
    }
  }

  return std::error_code{};
}

int main(int argc, char** argv) {
  std::string base_url = "ws://127.0.0.1:9001";
  std::string agent = "aero-client";

  if (argc >= 2) {
    base_url = argv[1];
  }
  if (argc >= 3) {
    agent = argv[2];
  }

#if _WIN32
  ::SetThreadUILanguage(MAKELANGID(LANG_ENGLISH, SUBLANG_ENGLISH_US));
#endif

  auto result_ec = run_autobahn_client(base_url, agent);
  if (result_ec) {
    std::println("Autobahn run failed: {}", result_ec.message());
    return 1;
  }

  std::println("Autobahn run finished.");
}

// int main() {}
