#pragma once

#include <deque>
#include <span>
#include <type_traits>

#include <asio/any_completion_handler.hpp>
#include <asio/any_io_executor.hpp>
#include <asio/as_tuple.hpp>
#include <asio/associated_cancellation_slot.hpp>
#include <asio/async_result.hpp>
#include <asio/bind_allocator.hpp>
#include <asio/bind_cancellation_slot.hpp>
#include <asio/bind_executor.hpp>
#include <asio/cancellation_signal.hpp>
#include <asio/co_composed.hpp>
#include <asio/connect.hpp>
#include <asio/deferred.hpp>
#include <asio/dispatch.hpp>
#include <asio/error.hpp>
#include <asio/ip/address.hpp>
#include <asio/ip/basic_endpoint.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/redirect_error.hpp>
#include <asio/strand.hpp>
#include <asio/use_awaitable.hpp>
#include <asio/write.hpp>

#if AERO_USE_TLS
#include "aero/tls/detail/alert_capture.hpp"
#include "aero/tls/detail/x509_verify_error.hpp"
#include "aero/tls/peer_identity.hpp"

#ifdef AERO_USE_WOLFSSL
#include "aero/tls/detail/wolfssl_error.hpp"
#endif

#include <asio/ssl.hpp>
#include <asio/ssl/error.hpp>
#include <asio/ssl/stream.hpp>
#include <asio/ssl/stream_base.hpp>
#endif

#include "aero/detail/aligned_allocator.hpp"
#include "aero/net/error.hpp"

namespace aero::net {

  class transport {
    using tcp = asio::ip::tcp;
    using tcp_socket = tcp::socket;
    using deferred_tcp_socket = asio::as_tuple_t<asio::deferred_t>::as_default_on_t<tcp_socket>;
    using deferred_tcp_resolver = asio::as_tuple_t<asio::deferred_t>::as_default_on_t<tcp::resolver>;
    using write_completion_handler = asio::any_completion_handler<void(std::error_code, std::size_t)>;

#if AERO_USE_TLS
    template <typename CompletionToken>
    auto async_handshake(CompletionToken&& token) {
      auto bound_token = asio::bind_allocator(aero::detail::aligned_allocator<>{}, std::forward<CompletionToken>(token));

      return asio::async_initiate<decltype(bound_token), void(std::error_code)>(
        asio::co_composed<void(std::error_code)>(
          [](auto, transport* self) -> void {
            co_await asio::dispatch(self->as_deferred_tuple());

            using tls::detail::x509_verify_error;
            tls::detail::alert_capture tls_alerts;
            tls_alerts.install(self->tls_stream_->native_handle());

            auto [handshake_ec] =
              co_await self->tls_stream_->async_handshake(asio::ssl::stream_base::client, self->as_deferred_tuple());

            co_return self->transform_handshake_error(handshake_ec, tls_alerts);
          },
          strand_),
        bound_token,
        this);
    }

    std::error_code handshake() {
      using tls::detail::x509_verify_error;
      tls::detail::alert_capture tls_alerts;
      tls_alerts.install(tls_stream_->native_handle());

      std::error_code handshake_ec;
      static_cast<void>(tls_stream_->handshake(asio::ssl::stream_base::client, handshake_ec));

      return transform_handshake_error(handshake_ec, tls_alerts);
    }
#endif

   public:
    using executor_type = asio::any_io_executor;

    explicit transport(asio::strand<executor_type> strand): strand_(std::move(strand)), socket_(strand_) {}

#if AERO_USE_TLS
    explicit transport(asio::strand<executor_type> strand, asio::ssl::context& context)
      : strand_(std::move(strand)), socket_(strand_), tls_stream_(std::in_place, socket_, context) {}
#endif

    template <typename CompletionToken>
    auto async_connect(std::string host, asio::ip::port_type port, CompletionToken&& token) {
      auto bound_token = asio::bind_allocator(aero::detail::aligned_allocator<>{}, std::forward<CompletionToken>(token));

      return asio::async_initiate<decltype(bound_token), void(std::error_code)>(
        asio::co_composed<void(std::error_code)>(
          [](auto, transport* self, std::string host, asio::ip::port_type port) -> void {
            co_await asio::dispatch(self->as_deferred_tuple());

            using net::connect_error;

            std::error_code address_parse_ec;
            auto address = asio::ip::make_address(host, address_parse_ec);

            bool using_address = !address_parse_ec;
            std::error_code connect_ec;

            if (using_address) {
              asio::ip::tcp::endpoint endpoint(address, port);
              std::tie(connect_ec) = co_await self->socket_.async_connect(endpoint, self->as_deferred_tuple());
            } else {
              deferred_tcp_resolver resolver{self->strand_};
              auto service = std::to_string(port);

              // GCC 15: Destructor of tuple-protocol structured binding from co_await skipped at -O1+
              // https://gcc.gnu.org/bugzilla/show_bug.cgi?id=124584
              auto resolve_result = co_await resolver.async_resolve(host, service, self->as_deferred_tuple());
              auto& [resolve_ec, resolved_endpoints] = resolve_result;
              if (resolve_ec) {
                if (resolve_ec == asio::error::bad_descriptor) {
                  co_return connect_error::host_invalid;
                }
                if (resolve_ec == asio::error::operation_aborted) {
                  co_return resolve_ec;
                }

                co_return connect_error::host_resolve_failed;
              }

              std::tie(connect_ec, std::ignore) =
                co_await asio::async_connect(self->socket_.lowest_layer(), resolved_endpoints, self->as_deferred_tuple());
            }

            if (connect_ec) {
              co_return connect_ec;
            }

#if AERO_USE_TLS
            if (self->is_using_tls_stream()) {
              if (!using_address) {
                if (auto ec = tls::set_sni(self->tls_stream_->native_handle(), host); ec) {
                  co_return ec;
                }

                if (auto ec = tls::set_expected_peer_host(self->tls_stream_->native_handle(), host); ec) {
                  co_return ec;
                }
              }

              co_return co_await self->async_handshake(self->as_deferred_tuple());
            }
#endif

            co_return std::error_code{};
          },
          strand_),
        bound_token,
        this,
        std::move(host),
        port);
    }

    std::error_code connect(std::string host, asio::ip::port_type port) {
      std::error_code connect_ec;
      std::error_code address_ec;
      auto address = asio::ip::make_address(host, address_ec);
      bool is_using_address = !static_cast<bool>(address_ec);

      if (is_using_address) {
        tcp::endpoint endpoint{address, port};
        static_cast<void>(socket_.connect(endpoint, connect_ec));
      } else {
        tcp::resolver resolver{strand_};
        std::error_code resolve_ec;
        auto resolved_endpoints = resolver.resolve(host, std::to_string(port), resolve_ec);
        if (resolve_ec) {
          if (resolve_ec == asio::error::bad_descriptor) {
            return connect_error::host_invalid;
          }

          return connect_error::host_resolve_failed;
        }

        asio::connect(socket_, resolved_endpoints, connect_ec);
      }

      if (connect_ec) {
        return connect_ec;
      }

#if AERO_USE_TLS
      if (is_using_tls_stream()) {
        if (!is_using_address) {
          if (auto ec = tls::set_sni(tls_stream_->native_handle(), host); ec) {
            return ec;
          }

          if (auto ec = tls::set_expected_peer_host(tls_stream_->native_handle(), host); ec) {
            return ec;
          }
        }

        return handshake();
      }
#endif

      return {};
    }

    template <typename CompletionToken>
    auto async_shutdown(CompletionToken&& token) {
      auto bound_token = asio::bind_allocator(aero::detail::aligned_allocator<>{}, std::forward<CompletionToken>(token));
      return asio::async_initiate<decltype(bound_token), void(std::error_code)>(
        asio::co_composed<void(std::error_code)>(
          [](auto, transport* self) -> void {
            co_await asio::dispatch(self->as_deferred_tuple());

            std::error_code shutdown_ec;
            std::error_code close_ec;

#if AERO_USE_TLS
            if (self->is_using_tls_stream()) {
              std::tie(shutdown_ec) = co_await self->tls_stream_->async_shutdown(self->as_deferred_tuple());
              if (is_ignorable_close_error(shutdown_ec)) {
                shutdown_ec.clear();
              }
            }
#endif

            static_cast<void>(self->lowest_layer().close(close_ec));
            if (is_ignorable_close_error(close_ec)) {
              close_ec.clear();
            }

            co_return shutdown_ec ? shutdown_ec : close_ec;
          },
          strand_),
        bound_token,
        this);
    }

    std::error_code shutdown() {
      std::error_code shutdown_ec;
      std::error_code close_ec;

#if AERO_USE_TLS
      if (is_using_tls_stream()) {
        static_cast<void>(tls_stream_->shutdown(shutdown_ec));
        if (is_ignorable_close_error(shutdown_ec)) {
          shutdown_ec.clear();
        }
      }
#endif

      static_cast<void>(socket_.close(close_ec));
      if (is_ignorable_close_error(close_ec)) {
        close_ec.clear();
      }

      return shutdown_ec ? shutdown_ec : close_ec;
    }

    template <typename MutableBuffersSequence, typename CompletionToken>
    auto async_read_some(const MutableBuffersSequence& buffers, CompletionToken&& token) {
      auto bound_token = asio::bind_allocator(aero::detail::aligned_allocator<>{}, std::forward<CompletionToken>(token));
      return asio::async_initiate<void(std::error_code, std::size_t)>(initiate_async_read_some{this}, bound_token, buffers);
    }

    template <typename MutableBuffersSequence>
    std::size_t read_some(const MutableBuffersSequence& buffers) {
#if AERO_USE_TLS
      if (is_using_tls_stream()) {
        return tls_stream_->read_some(buffers);
      }
#endif
      return socket_.read_some(buffers);
    }

    template <typename MutableBuffersSequence>
    std::size_t read_some(const MutableBuffersSequence& buffers, std::error_code& ec) {
#if AERO_USE_TLS
      if (is_using_tls_stream()) {
        return tls_stream_->read_some(buffers, ec);
      }
#endif
      return socket_.read_some(buffers, ec);
    }

    template <typename CompletionToken>
    auto async_write(std::span<const std::byte> buffer, CompletionToken&& token) {
      auto bound_token = asio::bind_allocator(aero::detail::aligned_allocator<>{}, std::forward<CompletionToken>(token));
      return asio::async_initiate<void(std::error_code, std::size_t)>(initiate_async_write{this}, bound_token, buffer);
    }

    template <typename ConstBufferSequence>
    std::size_t write_some(const ConstBufferSequence& buffers) {
#if AERO_USE_TLS
      if (is_using_tls_stream()) {
        return tls_stream_->write_some(buffers);
      }
#endif
      return socket_.write_some(buffers);
    }

    template <typename ConstBufferSequence>
    std::size_t write_some(const ConstBufferSequence& buffers, std::error_code& ec) {
#if AERO_USE_TLS
      if (is_using_tls_stream()) {
        return tls_stream_->write_some(buffers, ec);
      }
#endif
      return socket_.write_some(buffers, ec);
    }

    [[nodiscard]] typename deferred_tcp_socket::lowest_layer_type& lowest_layer() {
      // tcp::socket is a basic_socket, which declares lowest_layer_type as
      // itself, because basic_socket_type is always a lowest_layer.
      // asio::ssl::stream<tcp::socket> declares lowest_layer_type as
      // tcp::socket. This is important because it means that both branches
      // will always have the same return type in our case.
#if AERO_USE_TLS
      if (is_using_tls_stream()) {
        return tls_stream_->lowest_layer();
      }
#endif
      return socket_.lowest_layer();
    }

    [[nodiscard]] executor_type get_executor() const noexcept {
      return strand_;
    }

    [[nodiscard]] bool is_using_tls_stream() const noexcept {
#if AERO_USE_TLS
      return tls_stream_.has_value();
#else
      return false;
#endif
    }

    [[nodiscard]] bool is_open() const noexcept {
      return socket_.is_open();
    }

   private:
    [[nodiscard]] asio::executor_binder<asio::as_tuple_t<asio::deferred_t>, asio::strand<executor_type>> as_deferred_tuple() {
      return asio::bind_executor(strand_, asio::as_tuple(asio::deferred));
    }

    static bool is_ignorable_close_error(std::error_code ec) {
      return ec == asio::error::not_connected || ec == asio::error::eof || ec == asio::error::bad_descriptor
#if AERO_USE_TLS
             || ec == asio::ssl::error::stream_truncated
#endif
        ;
    }

#if AERO_USE_TLS
    std::error_code transform_handshake_error(std::error_code handshake_ec, tls::detail::alert_capture& alerts) noexcept {
      using tls::detail::x509_verify_error;

#ifdef AERO_USE_WOLFSSL
      if (handshake_ec) {
        auto wolfssl_error = ::SSL_get_error(tls_stream_->native_handle(), 0);
        if (auto cert_error = tls::detail::wolfssl_error_to_cert_error(wolfssl_error)) {
          return cert_error.value();
        }
      }
#endif

      auto verify_result = static_cast<x509_verify_error>(::SSL_get_verify_result(tls_stream_->native_handle()));
      if (verify_result != x509_verify_error::ok) {
        auto cert_error = tls::detail::verify_error_to_cert_error(verify_result);
        if (cert_error) {
          return cert_error.value();
        }
      }

      auto alert = alerts.get_last_tls_alert();
      if (alert) {
        auto alert_ec = tls::detail::tls_alert_to_error_code(*alert);
        if (alert_ec) {
          return alert_ec.value();
        }
      }

      return handshake_ec;
    }
#endif

    // Timeout adapters use executor exposed by the operation's initiation,
    // so initiator type should expose executor_type and .get_executor().
    // This means that a simple lambda won't be enough for this case
    struct initiate_async_read_some {
      transport* self;

      using executor_type = transport::executor_type;

      [[nodiscard]] executor_type get_executor() const noexcept {
        return self->strand_;
      }

      template <typename Handler, typename MutableBuffersSequence>
      void operator()(Handler&& completion_handler, const MutableBuffersSequence& buffers) const {
#if AERO_USE_TLS
        if (self->is_using_tls_stream()) {
          self->tls_stream_->async_read_some(buffers, std::forward<Handler>(completion_handler));
          return;
        }
#endif
        self->socket_.async_read_some(buffers, std::forward<Handler>(completion_handler));
      }
    };

    struct initiate_async_write {
      transport* self;

      using executor_type = transport::executor_type;

      [[nodiscard]] executor_type get_executor() const noexcept {
        return self->strand_;
      }

      template <typename Handler>
      void operator()(Handler&& completion_handler, std::span<const std::byte> buffer) const {
        // Dispatch to our strand in cases where the associated executor of the
        // passed completion_handler is not this strand
        asio::dispatch(self->strand_,
          [self = self, buffer, completion_handler = std::forward<Handler>(completion_handler)]() mutable {
            self->enqueue_write(asio::const_buffer(buffer.data(), buffer.size()), std::move(completion_handler));
          });
      }
    };

    struct pending_write {
      asio::const_buffer buffer;
      write_completion_handler completion;
      asio::cancellation_slot cancellation_slot;
      bool in_flight = false;
      bool cancelled = false;

      template <typename F>
      void try_assign_cancellation_slot(F&& fn)
        requires(std::invocable<F, asio::cancellation_type>)
      {
        if (cancellation_slot.is_connected()) {
          cancellation_slot.assign(std::forward<F>(fn));
        }
      }
    };

    void enqueue_write(asio::const_buffer buffer, write_completion_handler completion_handler) {
      bool is_any_write_in_flight = !pending_writes_.empty();

      auto caller_slot = asio::get_associated_cancellation_slot(completion_handler);
      auto& pending = pending_writes_.emplace_back(buffer, std::move(completion_handler), caller_slot);

      // Assign cancellation handler for the queued write.
      // It will invoke a completion_handler with operation_aborted, but will
      // still leave the pending_write in the deque, which will be cleared by
      // .pop_cancelled_writes_from_queue() when the queue reaches this entry
      pending.try_assign_cancellation_slot([this, &pending](asio::cancellation_type type) {
        if (type == asio::cancellation_type::none || pending.cancelled) {
          return;
        }
        pending.cancelled = true;

        if (pending.in_flight) {
          write_cancel_signal_.emit(type);
          return;
        }

        // Resetting cancellation_slot is necessary because cancellation_slot is
        // a handle to the caller's cancellation_signal, and by the time the
        // queue reaches cancellation_slot.clear() inside
        // .pop_cancelled_writes_from_queue(), the caller has already received
        // asio::error::operation_aborted and could have started a new operation
        // with the same cancellation_signal, meaning the new operation has
        // placed its own handler in that slot. If cancellation_slot is not
        // reset, then in this case, the .clear() call inside
        // .pop_cancelled_writes_from_queue() will destroy the cancellation
        // handler belonging to the new operation
        pending.cancellation_slot = asio::cancellation_slot{};

        // Use asio::post, not a direct call, so the caller's completion handler
        // does not run inside its own cancellation emit
        asio::post(strand_,
          [completion = std::move(pending.completion)]() mutable { std::move(completion)(asio::error::operation_aborted, 0); });
      });

      if (!is_any_write_in_flight) {
        start_write_loop();
      }
    }

    void start_write_loop() {
      pop_cancelled_front_writes_from_queue();
      if (pending_writes_.empty()) {
        return;
      }

      auto& pending = pending_writes_.front();
      pending.in_flight = true;

      auto on_write_completion = [this, completion = std::move(pending.completion)](std::error_code ec,
                                   std::size_t bytes_transferred) mutable {
        pending_writes_.front().cancellation_slot.clear();
        pending_writes_.pop_front();

        // asio::async_write is a composed operation that invokes
        // .async_write_some() N times, where N is usually unknown
        // (depends on the OS and TCP stack). If this operation fails or is
        // cancelled mid-flight - the stream is considered broken, since by
        // the non-copying aero contract, caller no longer has to provide a
        // write buffer, so we can't continue writing data after recovering
        // from an error
        if (ec) {
          std::error_code ignored_ec;
          static_cast<void>(socket_.close(ignored_ec));
          complete_all_queued_writes_with_error(asio::error::operation_aborted);
        } else {
          start_write_loop();
        }

        std::move(completion)(ec, bytes_transferred);
      };

#if AERO_USE_TLS
      if (is_using_tls_stream()) {
        asio::async_write(*tls_stream_,
          pending.buffer,
          asio::bind_cancellation_slot(write_cancel_signal_.slot(), std::move(on_write_completion)));
        return;
      }
#endif

      asio::async_write(socket_,
        pending.buffer,
        asio::bind_cancellation_slot(write_cancel_signal_.slot(), std::move(on_write_completion)));
    }

    void pop_cancelled_front_writes_from_queue() {
      while (!pending_writes_.empty() && pending_writes_.front().cancelled) {
        pending_writes_.front().cancellation_slot.clear();
        pending_writes_.pop_front();
      }
    }

    void complete_all_queued_writes_with_error(std::error_code ec) {
      while (!pending_writes_.empty()) {
        auto& pending = pending_writes_.front();
        if (!pending.cancelled) {
          asio::post(strand_, [ec, completion = std::move(pending.completion)]() mutable { std::move(completion)(ec, 0); });
        }

        pending_writes_.front().cancellation_slot.clear();
        pending_writes_.pop_front();
      }
    }

    asio::strand<asio::any_io_executor> strand_;
    deferred_tcp_socket socket_;
#if AERO_USE_TLS
    std::optional<asio::ssl::stream<deferred_tcp_socket&>> tls_stream_;
#endif

    // The front of the queue is the record currently in-flight. Only its own
    // completion removes it from the queue. Cancelled records are terminated in
    // place and are later skipped by the record loop, so references to queue
    // elements remain unchanged
    std::deque<pending_write> pending_writes_;
    asio::cancellation_signal write_cancel_signal_;
  };

} // namespace aero::net
