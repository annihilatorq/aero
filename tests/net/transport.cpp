#include "aero/net/transport.hpp"

#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/buffer.hpp>
#include <asio/cancel_after.hpp>
#include <asio/co_spawn.hpp>
#include <asio/deferred.hpp>
#include <asio/detached.hpp>
#include <asio/error.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/address.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/steady_timer.hpp>
#include <chrono>
#include <cstddef>
#include <span>
#include <system_error>
#include <ut/ut.hpp>
#include <utility>
#include <vector>

using namespace ut;
using namespace std::chrono_literals;

namespace {

  constexpr auto cancel_delay = 50ms;
  constexpr auto settle_delay = 250ms;
  constexpr auto context_run_timeout = 5s;

  std::span<const std::byte> unflushable_payload() {
    constexpr std::size_t larger_than_socket_buffers = 64ULL * 1024 * 1024;
    static const std::vector<std::byte> bytes(larger_than_socket_buffers);
    return bytes;
  }

  std::span<const std::byte> small_payload() {
    static const std::vector<std::byte> bytes(16);
    return bytes;
  }

  struct write_result {
    std::error_code ec;
    std::size_t bytes = 0;
    bool finished = false;
    bool is_socket_open = true;
  };

  struct loopback_transport {
    asio::io_context context;
    asio::ip::tcp::acceptor acceptor{context, {asio::ip::make_address("127.0.0.1"), 0}};
    asio::ip::tcp::socket peer{context};
    aero::net::transport<> transport{asio::make_strand(context.get_executor())};

    loopback_transport() {
      acceptor.async_accept(peer, [](std::error_code) {});
    }

    asio::awaitable<std::error_code> connect() {
      auto [ec] =
        co_await transport.async_connect("127.0.0.1", acceptor.local_endpoint().port(), asio::as_tuple(asio::deferred));
      expect(not ec) << "loopback connect failed: " << ec.message();
      co_return ec;
    }

    void spawn_write(std::span<const std::byte> payload, write_result& result) {
      asio::co_spawn(
        transport.get_executor(),
        [this, payload, &result]() -> asio::awaitable<void> {
          auto [ec, bytes] = co_await transport.async_write(payload, asio::as_tuple(asio::deferred));
          result = write_result{.ec = ec, .bytes = bytes, .finished = true, .is_socket_open = transport.is_open()};
        },
        asio::detached);
    }

    void spawn_write(std::span<const std::byte> payload, write_result& result, std::chrono::milliseconds cancel_after) {
      asio::co_spawn(
        transport.get_executor(),
        [this, payload, cancel_after, &result]() -> asio::awaitable<void> {
          auto [ec, bytes] =
            co_await transport.async_write(payload, asio::cancel_after(cancel_after, asio::as_tuple(asio::deferred)));
          result = write_result{.ec = ec, .bytes = bytes, .finished = true, .is_socket_open = transport.is_open()};
        },
        asio::detached);
    }

    void run(auto fn) {
      asio::co_spawn(transport.get_executor(), std::move(fn), asio::detached);
      context.run_for(context_run_timeout);
    }

    asio::awaitable<void> wait(std::chrono::milliseconds delay) {
      asio::steady_timer timer(context, delay);
      co_await timer.async_wait(asio::as_tuple(asio::deferred));
    }

    asio::awaitable<void> drain_peer_until_finished(const write_result& write) {
      std::vector<std::byte> sink(1024ULL * 1024);
      while (true) {
        auto [ec, drained] = co_await peer.async_read_some(asio::buffer(sink), asio::as_tuple(asio::deferred));
        if (ec || write.finished) {
          co_return;
        }
      }
    }

    void close_peer() {
      std::error_code ignored;
      static_cast<void>(peer.close(ignored));
    }
  };

} // namespace

int main() {
  suite transport_write_cancellation = [] {
    "cancelling a queued write completes it with operation_aborted and leaves the in-flight write running"_test = [] {
      loopback_transport loopback;
      write_result in_flight;
      write_result queued;

      loopback.run([&]() -> asio::awaitable<void> {
        if (co_await loopback.connect()) {
          co_return;
        }

        loopback.spawn_write(unflushable_payload(), in_flight);
        loopback.spawn_write(small_payload(), queued, cancel_delay);

        co_await loopback.wait(settle_delay);
        expect(not in_flight.finished) << "in-flight write must keep running after a queued write is cancelled";

        loopback.close_peer();
      });

      expect(queued.finished && queued.ec == asio::error::operation_aborted)
        << "queued write should complete with operation_aborted, got: " << queued.ec.message();
      expect(in_flight.finished && in_flight.ec) << "in-flight write should fail only once the peer closes the connection";
      expect(queued.is_socket_open) << "transport socket should still be open after queued write cancellation";
    };

    "cancelling the in-flight write completes the queued write with operation_aborted and closes the socket"_test = [] {
      loopback_transport loopback;
      write_result in_flight;
      write_result queued;

      loopback.run([&]() -> asio::awaitable<void> {
        if (co_await loopback.connect()) {
          co_return;
        }

        loopback.spawn_write(unflushable_payload(), in_flight, cancel_delay);
        loopback.spawn_write(small_payload(), queued);

        co_await loopback.wait(settle_delay);
        co_await loopback.drain_peer_until_finished(queued);
      });

      expect(in_flight.finished && in_flight.ec == asio::error::operation_aborted)
        << "in-flight write should complete with operation_aborted, got: " << in_flight.ec.message();
      expect(queued.finished && queued.ec == asio::error::operation_aborted)
        << "queued write should complete with operation_aborted, got: " << queued.ec.message();
      expect(not in_flight.is_socket_open && not queued.is_socket_open)
        << "transport should close its socket after in-flight write cancellation";
    };
  };
}
