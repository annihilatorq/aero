#pragma once

#include <chrono>
#include <string>
#include <system_error>
#include <thread>
#include <utility>

#include <asio/error.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

namespace aero::tests::net {

  constexpr static std::chrono::seconds accept_timeout{5};

  class blocking_server {
    using tcp = asio::ip::tcp;

   public:
    blocking_server(const blocking_server&) = delete;
    blocking_server& operator=(const blocking_server&) = delete;
    blocking_server(blocking_server&&) = delete;
    blocking_server& operator=(blocking_server&&) = delete;

    template <typename Fn>
    explicit blocking_server(Fn on_connection)
      : acceptor_(io_context_, resolve_localhost_endpoint()), thread_([this, on_connection = std::move(on_connection)] {
          tcp::socket peer{io_context_};
          std::error_code accept_ec = asio::error::timed_out;
          acceptor_.async_accept(peer, [&](std::error_code ec) { accept_ec = ec; });
          io_context_.run_for(accept_timeout);

          if (accept_ec) {
            return;
          }
          on_connection(peer);
        }) {}

    ~blocking_server() {
      join();
    }

    void join() {
      io_context_.stop();
      if (thread_.joinable()) {
        thread_.join();
      }
    }

    [[nodiscard]] std::string address() const {
      return acceptor_.local_endpoint().address().to_string();
    }

    [[nodiscard]] asio::ip::port_type port() const {
      return acceptor_.local_endpoint().port();
    }

   private:
    // Bind the listener to the first address to which "localhost" resolves
    // (::1 on Windows) instead of using "127.0.0.1". Tests that pass
    // "localhost" to `net::transport::connect` attempt to connect internally
    // to each of the resolved addresses in order, and Windows takes about 2
    // seconds to reject the loopback connection at an address that no one is
    // listening on
    [[nodiscard]] tcp::endpoint resolve_localhost_endpoint() {
      tcp::resolver resolver{io_context_};
      return *resolver.resolve("localhost", "0").begin();
    }

    asio::io_context io_context_;
    tcp::acceptor acceptor_;
    std::thread thread_;
  };

} // namespace aero::tests::net
