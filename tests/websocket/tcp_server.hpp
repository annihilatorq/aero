#pragma once

#include <cstddef>
#include <cstdint>
#include <exception>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>

#include <asio/io_context.hpp>
#include <asio/ip/address.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read.hpp>
#include <asio/read_until.hpp>
#include <asio/write.hpp>

#include "aero/http/detail/line_endings.hpp"
#include "aero/http/error.hpp"
#include "aero/http/headers.hpp"
#include "asio/socket_base.hpp"

using tcp = asio::ip::tcp;

struct connection {
  tcp::socket socket;
  std::string buffer;

  [[nodiscard]] std::string read_request() {
    auto head = read_request_head();
    return head + read_request_body(head);
  }

  void write_response(std::string_view response) {
    asio::write(socket, asio::buffer(response.data(), response.size()));
  }

  [[nodiscard]] std::string read_bytes(std::size_t count) {
    if (buffer.size() < count) {
      std::error_code error;
      asio::read(socket, asio::dynamic_buffer(buffer), asio::transfer_exactly(count - buffer.size()), error);
      if (error) {
        throw std::system_error{error};
      }
    }

    auto bytes = buffer.substr(0, count);
    buffer.erase(0, count);
    return bytes;
  }

  void close() {
    std::error_code ec;
    static_cast<void>(socket.shutdown(asio::socket_base::shutdown_both, ec));
    static_cast<void>(socket.close(ec));
  }

 private:
  [[nodiscard]] std::size_t parse_content_length(std::string_view request_head) {
    using namespace aero;
    using aero::http::detail::crlf;

    auto request_line_end = request_head.find(crlf);
    if (request_line_end == std::string_view::npos) {
      throw std::runtime_error{"request head has no request line"};
    }

    auto headers = http::headers::parse(request_head.substr(request_line_end + crlf.size()));
    if (!headers) {
      throw std::system_error{headers.error()};
    }

    auto length = aero::http::content_length<std::size_t>(*headers);
    if (length) {
      return *length;
    }
    if (length.error() == aero::http::header_error::content_length_missing) {
      return 0;
    }
    throw std::system_error{length.error()};
  }

  [[nodiscard]] std::string read_request_head() {
    std::error_code error;
    auto size = asio::read_until(socket, asio::dynamic_buffer(buffer), aero::http::detail::double_crlf, error);
    if (error) {
      throw std::system_error{error};
    }

    auto head = buffer.substr(0, size);
    buffer.erase(0, size);
    return head;
  }

  [[nodiscard]] std::string read_request_body(std::string_view head) {
    auto content_length = parse_content_length(head);
    if (buffer.size() < content_length) {
      std::error_code error;
      asio::read(socket, asio::dynamic_buffer(buffer), asio::transfer_exactly(content_length - buffer.size()), error);
      if (error) {
        throw std::system_error{error};
      }
    }

    auto body = buffer.substr(0, content_length);
    buffer.erase(0, content_length);
    return body;
  }

  [[nodiscard]] std::string request_body(std::string_view raw_request) {
    auto separator = raw_request.find(aero::http::detail::double_crlf);
    if (separator == std::string_view::npos) {
      throw std::runtime_error{"request has no header terminator"};
    }
    return std::string{raw_request.substr(separator + aero::http::detail::double_crlf.size())};
  }
};

class tcp_server final {
 public:
  explicit tcp_server(): acceptor_(io_context_, tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0)) {
    async_accept();
    thread_ = std::thread([this] { io_context_.run(); });
  }

  tcp_server(const tcp_server&) = delete;
  tcp_server& operator=(const tcp_server&) = delete;
  tcp_server(tcp_server&&) = delete;
  tcp_server& operator=(tcp_server&&) = delete;

  ~tcp_server() {
    asio::post(io_context_, [this] {
      std::error_code ignored;
      static_cast<void>(acceptor_.close(ignored));
    });

    thread_.join();
  }

  [[nodiscard]] std::uint16_t port() const {
    return acceptor_.local_endpoint().port();
  }

  void on_accept(std::function<void(std::shared_ptr<connection>)> handler) {
    accept_handler_ = handler;
  }

  void close_last_conn() {
    if (last_conn_ == nullptr) {
      return;
    }

    last_conn_->close();
    last_conn_.reset();
  }

  void join() {
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  [[nodiscard]] std::exception_ptr exception() const noexcept {
    return exception_;
  }

 private:
  void async_accept() {
    acceptor_.async_accept([this](std::error_code ec, asio::ip::tcp::socket socket) {
      if (ec) {
        return;
      }

      auto conn = std::make_shared<connection>(connection{.socket = std::move(socket)});

      last_conn_ = conn;
      try {
        accept_handler_(conn);
      } catch (const std::system_error& e) {
        exception_ = std::make_exception_ptr(e);
      }
      async_accept();
    });
  }

  std::function<void(std::shared_ptr<connection>)> accept_handler_;
  std::shared_ptr<connection> last_conn_;
  asio::io_context io_context_;
  tcp::acceptor acceptor_;
  std::exception_ptr exception_;
  std::thread thread_;
};
