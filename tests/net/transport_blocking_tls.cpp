#include <array>
#include <string>
#include <string_view>
#include <system_error>

#include <asio/buffer.hpp>
#include <asio/error.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read.hpp>
#include <asio/socket_base.hpp>
#include <asio/ssl.hpp>
#include <asio/ssl/error.hpp>
#include <asio/strand.hpp>
#include <asio/write.hpp>

#include <ut/ut.hpp>

#include "aero/net/transport.hpp"
#include "aero/tls/error.hpp"

#include "common/certificates/certificates.hpp"
#include "common/tls_context.hpp"
#include "net/blocking_server.hpp"

using namespace ut;

namespace certificates = aero::tests::certificates;
using aero::tests::net::blocking_server;
using aero::tls::certificate_error;
using tcp = asio::ip::tcp;
using tls_stream = asio::ssl::stream<tcp::socket&>;

namespace {

  auto describe(std::error_code ec) {
    return std::string{ec.category().name()} + ": " + ec.message();
  }

  void drain_until_closed(tcp::socket& peer) {
    std::array<char, 256> sink{};
    std::error_code read_ec;
    while (not read_ec) {
      peer.read_some(asio::buffer(sink), read_ec);
    }
  }

} // namespace

int main() {
  // The transport is intentionally connecting via "localhost" instead of the
  // direct "127.0.0.1" address because "localhost" is not an address.
  // Therefore, asio::ip::make_address() inside transport::connect() returns
  // an error when attempting to parse and determine the address type. This
  // forces the client to choose a host-based connection branch, where a
  // hostname lookup is performed using `asio::ip::tcp::resolver`, followed
  // by the configuration of SNI and hostname verification for the TLS
  // handshake
  //
  // Also, the "leaf_valid" certificate is issued for "DNS:localhost", and
  // when connecting via IP, the hostname verification is simply skipped

  asio::io_context io_context;
  auto make_transport = [&](asio::ssl::context& ctx) {
    return aero::net::transport{asio::make_strand(io_context.get_executor()), ctx};
  };

  suite transport_blocking_tls_connect = [&] {
    "connect completes the handshake against a trusted certificate"_test = [&] {
      auto server_ctx = aero::tests::make_tls_server_context();
      std::error_code server_handshake_ec;
      blocking_server server{[&](tcp::socket& peer) {
        tls_stream stream{peer, server_ctx};
        static_cast<void>(stream.handshake(asio::ssl::stream_base::server, server_handshake_ec));
      }};

      auto client_ctx = aero::tests::make_tls_client_context();
      auto transport = make_transport(client_ctx);
      auto ec = transport.connect("localhost", server.port());
      expect(not ec) << describe(ec);
      expect(transport.is_using_tls_stream());

      server.join();
      expect(not server_handshake_ec) << "server handshake failed: " << server_handshake_ec.message();
    };

    "connect reports cert_expired for an expired certificate"_test = [&] {
      auto server_ctx = aero::tests::make_tls_server_context(certificates::leaf_expired);
      blocking_server server{[&](tcp::socket& peer) {
        tls_stream stream{peer, server_ctx};
        std::error_code ignored;
        static_cast<void>(stream.handshake(asio::ssl::stream_base::server, ignored));
      }};

      auto client_ctx = aero::tests::make_tls_client_context();
      auto transport = make_transport(client_ctx);
      auto ec = transport.connect("localhost", server.port());
      expect(ec == certificate_error::cert_expired) << describe(ec);
    };

    "connect reports cert_hostname_mismatch for a certificate issued to another host"_test = [&] {
      auto server_ctx = aero::tests::make_tls_server_context(certificates::leaf_wrong_host);
      blocking_server server{[&](tcp::socket& peer) {
        tls_stream stream{peer, server_ctx};
        std::error_code ignored;
        static_cast<void>(stream.handshake(asio::ssl::stream_base::server, ignored));
      }};

      auto client_ctx = aero::tests::make_tls_client_context();
      auto transport = make_transport(client_ctx);
      auto ec = transport.connect("localhost", server.port());
      expect(ec == certificate_error::cert_hostname_mismatch) << describe(ec);
    };
  };

  suite transport_blocking_tls_io = [&] {
    "write_some and read_some carry plaintext through the TLS stream"_test = [&] {
      auto server_ctx = aero::tests::make_tls_server_context();
      std::string received(5, '\0');
      std::error_code server_ec;
      blocking_server server{[&](tcp::socket& peer) {
        tls_stream stream{peer, server_ctx};
        static_cast<void>(stream.handshake(asio::ssl::stream_base::server, server_ec));
        if (server_ec) {
          return;
        }
        asio::read(stream, asio::buffer(received), server_ec);
        if (server_ec) {
          return;
        }
        asio::write(stream, asio::buffer(received), server_ec);
      }};

      auto client_ctx = aero::tests::make_tls_client_context();
      auto transport = make_transport(client_ctx);
      auto connect_ec = transport.connect("localhost", server.port());
      expect(not connect_ec) << describe(connect_ec);

      std::error_code write_ec;
      auto written = transport.write_some(asio::buffer(std::string_view{"hello"}), write_ec);
      expect(not write_ec) << "write_some failed: " << write_ec.message();
      expect(written == 5U) << "write_some wrote " << written << " bytes";

      std::array<char, 16> sink{};
      std::error_code read_ec;
      auto bytes = transport.read_some(asio::buffer(sink), read_ec);
      expect(not read_ec) << "read_some failed: " << read_ec.message();
      expect(std::string_view(sink.data(), bytes) == "hello");

      server.join();
      expect(not server_ec) << "server failed: " << server_ec.message();
      expect(received == "hello") << "server decrypted: " << received;
    };
  };

  suite transport_blocking_tls_shutdown = [&] {
    "shutdown sends close_notify so the peer reads a clean eof"_test = [&] {
      auto server_ctx = aero::tests::make_tls_server_context();
      std::error_code server_read_ec;
      blocking_server server{[&](tcp::socket& peer) {
        tls_stream stream{peer, server_ctx};
        std::error_code ignored;
        static_cast<void>(stream.handshake(asio::ssl::stream_base::server, ignored));

        std::array<char, 1> sink{};
        stream.read_some(asio::buffer(sink), server_read_ec);
        static_cast<void>(stream.shutdown(ignored));
      }};

      auto client_ctx = aero::tests::make_tls_client_context();
      auto transport = make_transport(client_ctx);
      auto connect_ec = transport.connect("localhost", server.port());
      expect(not connect_ec) << describe(connect_ec);

      auto shutdown_ec = transport.shutdown();
      expect(not shutdown_ec) << describe(shutdown_ec);
      expect(not transport.is_open());

      server.join();
      expect(server_read_ec == asio::error::eof) << "peer read got: " << describe(server_read_ec);
    };

    "shutdown succeeds after the peer closed TCP without close_notify"_test = [&] {
      auto server_ctx = aero::tests::make_tls_server_context();
      blocking_server server{[&](tcp::socket& peer) {
        tls_stream stream{peer, server_ctx};
        std::error_code ignored;
        static_cast<void>(stream.handshake(asio::ssl::stream_base::server, ignored));

        static_cast<void>(peer.shutdown(asio::socket_base::shutdown_send, ignored));
        drain_until_closed(peer);
      }};

      auto client_ctx = aero::tests::make_tls_client_context();
      auto transport = make_transport(client_ctx);
      auto connect_ec = transport.connect("localhost", server.port());
      expect(not connect_ec) << describe(connect_ec);

      std::array<char, 1> sink{};
      std::error_code read_ec;
      transport.read_some(asio::buffer(sink), read_ec);
      expect(read_ec == asio::ssl::error::stream_truncated) << "read_some got: " << describe(read_ec);

      auto shutdown_ec = transport.shutdown();
      expect(not shutdown_ec) << describe(shutdown_ec);
      expect(not transport.is_open());
    };
  };
}
