#include <array>
#include <asio/ip/basic_endpoint.hpp>
#include <cstddef>
#include <string>
#include <string_view>
#include <system_error>

#include <asio/buffer.hpp>
#include <asio/error.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/address.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read.hpp>
#include <asio/strand.hpp>
#include <asio/write.hpp>

#include <ut/ut.hpp>

#include "aero/net/error.hpp"
#include "aero/net/transport.hpp"

#include "net/blocking_server.hpp"

using namespace ut;

using aero::tests::net::blocking_server;
using tcp = asio::ip::tcp;

namespace {

  std::string read_exactly(tcp::socket& peer, std::size_t count, std::error_code& ec) {
    std::string bytes(count, '\0');
    asio::read(peer, asio::buffer(bytes), ec);
    return bytes;
  }

} // namespace

int main() {
  asio::io_context io_context;
  auto make_transport = [&] {
    return aero::net::transport{asio::make_strand(io_context.get_executor())};
  };

  suite transport_blocking_connect = [&] {
    "connect by ip address opens the socket"_test = [&] {
      blocking_server server{[](tcp::socket&) {}};
      auto transport = make_transport();

      auto ec = transport.connect(server.address(), server.port());
      expect(not ec) << "connect failed: " << ec.message();
      expect(transport.is_open());
    };

    "connect by host name resolves it and opens the socket"_test = [&] {
      blocking_server server{[](tcp::socket&) {}};
      auto transport = make_transport();

      auto ec = transport.connect("localhost", server.port());
      expect(not ec) << "connect failed: " << ec.message();
      expect(transport.is_open());
    };

    "connect to a port nobody listens on fails with connection_refused"_test = [&] {
      auto transport = make_transport();

      asio::ip::port_type closed_server_port = [] {
        asio::io_context io_context;
        tcp::acceptor acceptor{io_context, tcp::endpoint{asio::ip::make_address("127.0.0.1"), 0}};
        return acceptor.local_endpoint().port();
      }();

      // This takes ~2 seconds on Windows, impossible to fix
      auto ec = transport.connect("127.0.0.1", closed_server_port);
      expect(ec == asio::error::connection_refused) << "got: " << ec.message();
    };

    "connect to an unresolvable host name fails"_test = [&] {
      auto transport = make_transport();

      auto ec = transport.connect("nonexistent.invalid.aero", 80);
      expect(ec == aero::net::connect_error::host_resolve_failed) << "got: " << ec.message();
      expect(not transport.is_open());
    };
  };

  suite transport_blocking_io = [&] {
    "write_some delivers bytes to the peer"_test = [&] {
      std::error_code peer_read_ec;
      std::string received;
      blocking_server server{[&](tcp::socket& peer) { received = read_exactly(peer, 5, peer_read_ec); }};
      auto transport = make_transport();

      auto connect_ec = transport.connect(server.address(), server.port());
      expect(not connect_ec) << "connect failed: " << connect_ec.message();

      std::error_code write_ec;
      auto written = transport.write_some(asio::buffer(std::string_view{"hello"}), write_ec);
      expect(not write_ec) << "write_some failed: " << write_ec.message();
      expect(written == 5U) << "write_some wrote " << written << " bytes";

      server.join();
      expect(not peer_read_ec) << "peer read failed: " << peer_read_ec.message();
      expect(received == "hello") << "peer received: " << received;
    };

    "read_some returns bytes sent by the peer"_test = [&] {
      blocking_server server{[](tcp::socket& peer) { asio::write(peer, asio::buffer(std::string_view{"pong"})); }};
      auto transport = make_transport();

      auto connect_ec = transport.connect(server.address(), server.port());
      expect(not connect_ec) << "connect failed: " << connect_ec.message();

      std::array<char, 16> sink{};
      std::error_code read_ec;
      auto received = transport.read_some(asio::buffer(sink), read_ec);
      expect(not read_ec) << "read_some failed: " << read_ec.message();
      expect(std::string_view(sink.data(), received) == "pong");
    };

    "read_some reports eof after the peer closes"_test = [&] {
      blocking_server server{[](tcp::socket& peer) { peer.close(); }};
      auto transport = make_transport();

      auto connect_ec = transport.connect(server.address(), server.port());
      expect(not connect_ec) << "connect failed: " << connect_ec.message();

      std::array<char, 16> sink{};
      std::error_code read_ec;
      auto received = transport.read_some(asio::buffer(sink), read_ec);
      expect(read_ec == asio::error::eof) << "got: " << read_ec.message();
      expect(received == 0U);
    };

    "read_some without an error_code throws system_error carrying eof"_test = [&] {
      blocking_server server{[](tcp::socket& peer) { peer.close(); }};
      auto transport = make_transport();

      auto connect_ec = transport.connect(server.address(), server.port());
      expect(not connect_ec) << "connect failed: " << connect_ec.message();

      std::array<char, 16> sink{};
      std::error_code thrown;
      try {
        transport.read_some(asio::buffer(sink));
      } catch (const std::system_error& e) {
        thrown = e.code();
      }
      expect(thrown == asio::error::eof) << "got: " << thrown.message();
    };

    "read_some on a transport that never connected fails with bad_descriptor"_test = [&] {
      auto transport = make_transport();

      std::array<char, 16> sink{};
      std::error_code read_ec;
      transport.read_some(asio::buffer(sink), read_ec);
      expect(read_ec == asio::error::bad_descriptor) << "got: " << read_ec.message();
    };

    "write_some on a transport that never connected fails with bad_descriptor"_test = [&] {
      auto transport = make_transport();

      std::error_code write_ec;
      transport.write_some(asio::buffer(std::string_view{"hello"}), write_ec);
      expect(write_ec == asio::error::bad_descriptor) << "got: " << write_ec.message();
    };

    "write_some without an error_code throws system_error carrying bad_descriptor"_test = [&] {
      auto transport = make_transport();

      std::error_code thrown;
      try {
        transport.write_some(asio::buffer(std::string_view{"hello"}));
      } catch (const std::system_error& e) {
        thrown = e.code();
      }
      expect(thrown == asio::error::bad_descriptor) << "got: " << thrown.message();
    };
  };

  suite transport_blocking_shutdown = [&] {
    "shutdown closes the socket and the peer reads eof"_test = [&] {
      std::error_code peer_read_ec;
      blocking_server server{[&](tcp::socket& peer) { read_exactly(peer, 1, peer_read_ec); }};
      auto transport = make_transport();

      auto connect_ec = transport.connect(server.address(), server.port());
      expect(not connect_ec) << "connect failed: " << connect_ec.message();

      auto shutdown_ec = transport.shutdown();
      expect(not shutdown_ec) << "shutdown failed: " << shutdown_ec.message();
      expect(not transport.is_open());

      server.join();
      expect(peer_read_ec == asio::error::eof) << "peer got: " << peer_read_ec.message();
    };

    "shutdown after the peer closed succeeds"_test = [&] {
      blocking_server server{[](tcp::socket& peer) { peer.close(); }};
      auto transport = make_transport();

      auto connect_ec = transport.connect(server.address(), server.port());
      expect(not connect_ec) << "connect failed: " << connect_ec.message();

      std::array<char, 1> sink{};
      std::error_code read_ec;
      transport.read_some(asio::buffer(sink), read_ec);
      expect(read_ec == asio::error::eof) << "got: " << read_ec.message();

      auto shutdown_ec = transport.shutdown();
      expect(not shutdown_ec) << "shutdown failed: " << shutdown_ec.message();
      expect(not transport.is_open());
    };

    "shutdown on a transport that never connected succeeds"_test = [&] {
      auto transport = make_transport();

      auto shutdown_ec = transport.shutdown();
      expect(not shutdown_ec) << "shutdown failed: " << shutdown_ec.message();
      expect(not transport.is_open());
    };

    "shutdown twice succeeds"_test = [&] {
      blocking_server server{[](tcp::socket&) {}};
      auto transport = make_transport();

      auto connect_ec = transport.connect(server.address(), server.port());
      expect(not connect_ec) << "connect failed: " << connect_ec.message();

      expect(not transport.shutdown());
      auto second_ec = transport.shutdown();
      expect(not second_ec) << "second shutdown failed: " << second_ec.message();
    };
  };
}
