#pragma once

#include <cstddef>
#include <expected>
#include <vector>

#include "aero/http/headers.hpp"
#include "aero/http/status.hpp"
#include "aero/http/status_line.hpp"

namespace aero::http {

  struct response {
    std::vector<std::byte> body;
    http::status_line status_line;
    http::headers headers;

    [[nodiscard]] http::status status_code() const {
      return status_line.status_code;
    }

    [[nodiscard]] std::string_view text() const noexcept {
      return {reinterpret_cast<const char*>(body.data()), body.size()};
    }

    [[nodiscard]] std::span<const std::byte> bytes() const noexcept {
      return body;
    }

    [[nodiscard]] bool empty() const noexcept {
      return body.empty() && status_line.empty() && headers.empty();
    }

    [[nodiscard]] std::expected<std::string_view, std::error_code> content_type() const noexcept {
      return http::content_type(headers);
    }

    [[nodiscard]] std::string serialize() const {
      auto status_line_str = status_line.serialize();
      if (status_line_str.empty()) {
        return {};
      }

      auto headers_str = headers.serialize();
      if (headers_str.empty()) {
        return {};
      }

      std::string buffer;
      buffer.reserve(status_line_str.size() + headers_str.size() + body.size());
      buffer.append(status_line_str).append(headers_str).append(text());

      return buffer;
    }

    [[nodiscard]] explicit operator bool() const noexcept {
      return not empty();
    }
  };

  namespace detail {

    [[nodiscard]] inline std::pair<std::error_code, http::response> parse_response_partial(std::string_view str) {
      http::response response;

      auto status_line_end = str.find(http::detail::crlf);
      if (status_line_end == std::string_view::npos) {
        return {http::protocol_error::status_line_invalid, {}};
      }

      auto status_line = http::status_line::parse(str.substr(0, status_line_end));
      if (!status_line) {
        return {status_line.error(), {}};
      }

      response.status_line = *status_line;

      auto headers_section_start = status_line_end + http::detail::crlf.size();
      auto response_headers = http::headers::parse(str.substr(headers_section_start));
      if (!response_headers) {
        return {response_headers.error(), std::move(response)};
      }

      response.headers = *response_headers;

      return {{}, std::move(response)};
    }

  } // namespace detail

} // namespace aero::http
