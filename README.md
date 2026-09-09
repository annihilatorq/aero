<img width="1920" height="400" alt="Github Aero Readme" src="https://github.com/user-attachments/assets/1a5eb6a9-6383-4d0b-8827-7b1614ca3b94" />

<h1 align="center">Aero: Modern C++ Networking Library</h1>

<p align="center">
  <strong>Networking that feels just right.</strong>
  <br />
  C++23 WebSocket-first library. HTTP/1.x server support coming soon.
</p>

<p align="center">
  <a href="https://github.com/blazeauth/aero/actions/workflows/autobahn-websocket-client.yml"><img alt="Autobahn WebSocket client" src="https://github.com/blazeauth/aero/actions/workflows/autobahn-websocket-client.yml/badge.svg" /></a>
  <a href="https://github.com/blazeauth/aero/actions/workflows/windows-msvc.yml"><img alt="Windows MSVC" src="https://github.com/blazeauth/aero/actions/workflows/windows-msvc.yml/badge.svg" /></a>
  <a href="https://github.com/blazeauth/aero/actions/workflows/linux-gcc.yml"><img alt="Linux GCC" src="https://github.com/blazeauth/aero/actions/workflows/linux-gcc.yml/badge.svg" /></a>
  <a href="https://github.com/blazeauth/aero/actions/workflows/linux-clang.yml"><img alt="Linux Clang" src="https://github.com/blazeauth/aero/actions/workflows/linux-clang.yml/badge.svg" /></a>
  <a href="https://github.com/blazeauth/aero/actions/workflows/macos-appleclang.yml"><img alt="macOS AppleClang" src="https://github.com/blazeauth/aero/actions/workflows/macos-appleclang.yml/badge.svg" /></a>
</p>

<p align="center">
  <code>header-only</code>
  <code>C++23</code>
  <code>networking</code>
  <code>websocket</code>
  <code>http</code>
</p>

---

Aero is a lightweight, header-only networking library for modern C++.

Library provides an API for working with a WebSocket client, with optional TLS support. We plan to add HTTP/1.0 and HTTP/1.1 server support in the near future, and to further develop the library so that it can simultaneously offer a lightweight client component and a feature-rich, high-performance server framework.

The library compiles with either OpenSSL or wolfSSL. The asynchronous model is built on asio completion tokens and feels like an extension of asio rather than part of a different library, in other words, the library is designed to feel like a pleasant addition to asio, but it doesn't adhere to the asio style as strictly as, say, Boost-Beast.

> [!WARNING]
>
> Aero is still in active development, so you should expect changes to the API at least until it exits the pre-major release phase.

Here's an example of a WebSocket echo client:
```cpp
#include <print>

#include "aero/websocket/client.hpp"
#include "aero/websocket/close_code.hpp"

namespace websocket = aero::websocket;

int main() {
  websocket::client client;

  auto [connect_ec, handshake_response] = client.connect("ws://websockets.chilkat.io/wsChilkatEcho.ashx");
  if (connect_ec) {
    std::println("Connect failed: {}", connect_ec.message());
    return 1;
  }

  std::println("Connected. HTTP status: {} ({})",
    handshake_response.status_line.reason_phrase,
    handshake_response.status_code());

  if (auto send_ec = client.send_text("hello from aero client")) {
    std::println("Send failed: {}", send_ec.message());
    return 1;
  }

  auto message = client.read();
  if (!message) {
    std::println("Read failed: {}", message.error().message());
    return 1;
  }

  std::println("Echo: {}", message->text());

  if (auto close_ec = client.close(websocket::close_code::normal)) {
    std::println("Close failed: {}", close_ec.message());
  }
}
```

### More examples:
#### WebSocket Client
<details>
<summary>WebSocket Secure Binance Stream (<strong>Sync</strong>)</summary>

```cpp
#include <print>

#include "aero/error.hpp"
#include "aero/tls/system_context.hpp"
#include "aero/tls/version.hpp"
#include "aero/util/deadline.hpp"
#include "aero/websocket/client.hpp"
#include "aero/websocket/close_code.hpp"
#include "aero/websocket/message.hpp"

namespace websocket = aero::websocket;
namespace tls = aero::tls;
namespace http = aero::http;

void print_error(std::string_view message, const std::error_code& ec) {
  std::println("{}: {} ({} - {})", message, ec.message(), ec.value(), ec.category().name());
}

void print_headers(const http::headers& headers) {
  std::println("[HEADERS] Printing:");
  for (const auto& [name, value] : headers) {
    std::println("{}: {}", name, value);
  }
  std::println("[HEADERS] Done");
}

int main() {
  using namespace std::chrono_literals;

  // System context simply wraps asio::ssl::context and implements
  // AIA fetching for Win32, otherwise sets default verify paths
  tls::system_context tls_ctx{tls::version::tlsv1_2};
  tls_ctx.disable_deprecated_versions();

  websocket::client client{tls_ctx.context()};

  auto [connect_ec, handshake_response] = client.connect("wss://stream.binance.com:9443/ws/btcusdt@trade", 5s);
  if (connect_ec) {
    print_error("Connect to binance stream failed", connect_ec);
    return 1;
  }

  std::println("Succesfully connected");
  print_headers(handshake_response.headers);

  aero::deadline deadline{5min};

  for (;;) {
    if (deadline.expired()) {
      break;
    }

    auto message = client.read(deadline.remaining());
    if (!message) {
      if (message.error() == aero::errc::timeout && deadline.expired()) {
        std::println("Read deadline expired, breaking from read-loop");
        break;
      }
      print_error("Failed to receive message from binance stream", message.error());
      break;
    }

    if (!message->is_text()) {
      std::println("Received non-text message type ({}), skipping", message->kind);
      continue;
    }

    std::println("Received message from binance stream: {}", message->text());
  }

  auto close_ec = client.close(websocket::close_code::normal, "thank you, we are leaving.");
  if (close_ec) {
    print_error("Close handshake failed", close_ec);
    std::ignore = client.force_close();
  }

  return 0;
}
```
</details>

<details>
<summary>WebSocket Secure Postman Echo (<strong>Coro</strong>)</summary>

```cpp
#include <chrono>
#include <future>
#include <print>
#include <system_error>

#include <asio/awaitable.hpp>
#include <asio/use_awaitable.hpp>
#include <asio/use_future.hpp>

#include "aero/http/headers.hpp"
#include "aero/tls/initialize.hpp"
#include "aero/tls/system_context.hpp"
#include "aero/tls/version.hpp"
#include "aero/util/io_runtime.hpp"
#include "aero/websocket/client.hpp"
#include "aero/websocket/close_code.hpp"

using namespace std::chrono_literals;
namespace websocket = aero::websocket;

void print_error(std::string_view message, const std::error_code& ec) {
  std::println("{}: {} ({} - {})", message, ec.message(), ec.value(), ec.category().name());
}

void print_headers(const aero::http::headers& headers) {
  std::println("[HEADERS] Printing:");
  for (const auto& [name, value] : headers) {
    std::println("{}: {}", name, value);
  }
  std::println("[HEADERS] Done");
}

asio::awaitable<std::error_code> async_run_echo_client(websocket::client& client) {
  // https://blog.postman.com/introducing-postman-websocket-echo-service/
  auto [connect_ec, handshake_response] =
    co_await client.async_connect("wss://ws.postman-echo.com/raw", asio::as_tuple(asio::use_awaitable));
  if (connect_ec) {
    co_return connect_ec;
  }

  print_headers(handshake_response.headers);

  auto [write_ec] = co_await client.async_send_text("hello from aero client!!!", asio::as_tuple(asio::use_awaitable));
  if (write_ec) {
    co_return write_ec;
  }

  auto [read_ec, message] = co_await client.async_read(asio::cancel_after(1500ms, asio::as_tuple(asio::use_awaitable)));
  if (read_ec) {
    co_return read_ec;
  }

  std::println("Received message from postman echo server. Kind: {}. Text: {}", message.kind, message.text());

  auto [close_ec] = co_await client.async_close(websocket::close_code::normal, asio::as_tuple(asio::use_awaitable));
  if (close_ec) {
    if (close_ec == aero::errc::timeout) {
      co_await client.async_force_close(asio::use_awaitable);
      co_return std::error_code{};
    }
    co_return close_ec;
  }

  co_return std::error_code{};
}

int main() {
  // If using wolfSSL, this step is required
  aero::tls::initialize_library();

  aero::io_runtime runtime(1, aero::wait_threads);

  aero::tls::system_context tls_context{aero::tls::version::tlsv1_2};
  tls_context.disable_deprecated_versions();

  websocket::client client{runtime.get_executor(), tls_context.context()};

  try {
    // All coroutines should use client executor to serialize all
    // of the operations correctly & prevent any race conditions
    auto echo_ec = asio::co_spawn(client.get_executor(), async_run_echo_client(client), asio::use_future).get();
    if (echo_ec) {
      print_error("Postman echo client failed", echo_ec);
    }
  } catch (const std::system_error& e) {
    print_error("System error exception catched", e.code());
  } catch (const std::future_error& e) {
    print_error("Future error exception catched", e.code());
  }
}
```
</details>

<details>
<summary>WebSocket Chilkat Echo (<strong>Sync</strong>)</summary>

```cpp
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
  using namespace std::chrono_literals;
  websocket::client client;

  auto [connect_ec, handshake_response] = client.connect("ws://websockets.chilkat.io/wsChilkatEcho.ashx", 5s);
  if (connect_ec) {
    if (connect_ec == aero::errc::timeout) {
      print_error("Connect to echo server timed out", connect_ec);
      return 1;
    }

    print_error("Connect to echo server failed", connect_ec);
    return 1;
  }

  std::println("Connected. HTTP status: {} ({})",
    handshake_response.status_line.reason_phrase,
    handshake_response.status_code());

  auto text_ec = client.send_text("hello from aero client");
  if (text_ec) {
    print_error("Text send failed", text_ec);
    return 1;
  }

  auto read_result = client.read(1500ms);
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
```
</details>

---

### Why Aero?
We created Aero as a hobby project because existing solutions either depended on Boost.Asio or other heavyweight interfaces. Don't get us wrong, Boost.Beast is an excellent library. The only issue we see with using it as a client-side library is its dependency on Boost.Asio and the Boost ecosystem as a whole, as well as the lack of support for standalone-asio, which makes the library feel rather heavy for potentially simple projects. Aero uses standalone Asio and exposes operations via the standard Asio completion-token model. When built with WolfSSL it can produce pretty small binaries, making it suitable for resource‑constrained applications.

### Async model
Aero uses Asio-style completion tokens instead of inventing its own async model.

That means you can use the style that already fits your codebase:

- callbacks
- `asio::use_future`
- `asio::use_awaitable`
- other compatible completion token patterns

#### Synchronous API behavior

> [!IMPORTANT]
>
> Any synchronous function is implemented using `std::future` from an asynchronous function. If you try to call a synchronous function on the same thread on which the executor passed to the client is running, you will get the `aero::basic_error::deadlock_would_occur` error.

## TLS support

TLS is optional.

Aero can be built without TLS when you want the lightest possible setup, or with a TLS backend when you need secure HTTP / WebSocket connections.

Supported configurations include:

- OpenSSL
- wolfSSL (needs the OpenSSL compatibility layer)
- no TLS

## Important API contract notes

These details matter and are worth reading before using the async API heavily.

### Non-copying async write contract

Async write-side operations use a non-copying contract.

If you pass data into operations such as:

- `async_send_text`
- `async_send_binary`
- `async_ping`
- `async_pong`
- close reason strings for async close paths

that data must stay alive until the completion handler is called.

## WebSocket interface

Websocket connection `aero::websocket::basic_connection`:
```cpp
template <websocket::role Role>
class basic_connection {
 public:
  using transport_type = aero::net::transport;
  using duration = std::chrono::steady_clock::duration;
  using executor_type = typename transport_type::executor_type;

  explicit basic_connection(connection_options options = {});
  explicit basic_connection(executor_type executor, connection_options options = {});

  // Available only in TLS builds
  explicit basic_connection(asio::ssl::context& ssl_ctx);
  explicit basic_connection(connection_options options, asio::ssl::context& ssl_ctx);
  explicit basic_connection(executor_type executor, asio::ssl::context& ssl_ctx);
  explicit basic_connection(executor_type executor, connection_options options, asio::ssl::context& ssl_ctx);

  auto async_connect(urls::url url, CompletionToken&& token);
  auto async_connect(std::expected<urls::url, std::error_code> parsed_url, CompletionToken&& token);
  auto async_connect(std::string_view url, CompletionToken&& token);

  auto async_connect(urls::url url, http::headers headers, CompletionToken&& token);
  auto async_connect(std::expected<urls::url, std::error_code> parsed_url, http::headers headers, CompletionToken&& token);
  auto async_connect(std::string_view url, http::headers headers, CompletionToken&& token);

  auto async_send_text(std::string_view text, CompletionToken&& token);
  auto async_send_binary(std::span<const std::byte> data, CompletionToken&& token);

  auto async_ping(std::string_view text, CompletionToken&& token);
  auto async_ping(CompletionToken&& token);
  auto async_ping(std::span<const std::byte> data, CompletionToken&& token);

  auto async_pong(std::span<const std::byte> data, CompletionToken&& token);
  auto async_pong(std::string_view text, CompletionToken&& token);
  auto async_pong(CompletionToken&& token);

  auto async_close(websocket::close_code code, std::string_view reason, CompletionToken&& token);
  auto async_close(websocket::close_code code, CompletionToken&& token);
  auto async_force_close(CompletionToken&& token);

  auto async_read(CompletionToken&& token);

  std::tuple<std::error_code, http::response> connect(urls::url url);
  std::tuple<std::error_code, http::response> connect(urls::url url, duration timeout);
  std::tuple<std::error_code, http::response> connect(std::expected<urls::url, std::error_code> parsed_url);
  std::tuple<std::error_code, http::response> connect(std::expected<urls::url, std::error_code> parsed_url, duration timeout);
  std::tuple<std::error_code, http::response> connect(std::string_view url_string);
  std::tuple<std::error_code, http::response> connect(std::string_view url_string, duration timeout);

  std::tuple<std::error_code, http::response> connect(urls::url url, http::headers headers);
  std::tuple<std::error_code, http::response> connect(urls::url url, http::headers headers, duration timeout);
  std::tuple<std::error_code, http::response> connect(std::expected<urls::url, std::error_code> parsed_url, http::headers headers);
  std::tuple<std::error_code, http::response> connect(std::expected<urls::url, std::error_code> parsed_url, http::headers headers, duration timeout);
  std::tuple<std::error_code, http::response> connect(std::string_view url_string, http::headers headers);
  std::tuple<std::error_code, http::response> connect(std::string_view url_string, http::headers headers, duration timeout);

  std::error_code send_text(std::string_view text);
  std::error_code send_binary(std::span<const std::byte> data);

  std::error_code ping();
  std::error_code ping(std::string_view text);
  std::error_code ping(std::span<const std::byte> data);

  std::error_code pong();
  std::error_code pong(std::string_view text);
  std::error_code pong(std::span<const std::byte> data);

  std::error_code close(websocket::close_code code);
  std::error_code close(websocket::close_code code, std::string_view reason);
  std::error_code force_close();

  std::expected<websocket::message, std::error_code> read();
  std::expected<websocket::message, std::error_code> read(duration timeout);

  [[nodiscard]] bool is_open_for_writing() const noexcept;
  [[nodiscard]] bool is_connecting() const noexcept;
  [[nodiscard]] bool is_closed() const noexcept;
  [[nodiscard]] bool is_closing() const noexcept;
  [[nodiscard]] bool is_transport_secure() const noexcept;
  [[nodiscard]] executor_type get_executor() const noexcept;
  [[nodiscard]] asio::strand<executor_type> get_strand() const noexcept;
};
```

The synchronous API uses `std::error_code`, `std::expected` or `std::tuple` depending on the amount of data an operation needs to return. `async_connect(...)` completes with `http::response`, and `connect(...)` returns `std::tuple<std::error_code, http::response>`. When the HTTP status line and headers were parsed but WebSocket challenge validation failed, `ec` is non-zero and the returned response still contains the parsed peer response. By design, the API cannot throw exceptions (although exceptions possibly can be thrown by asio).

### TLS
- The transport is selected from the URL scheme on every `connect(...)`. `ws://` selects a plain TCP transport, `wss://` - TLS.
- A TLS transport uses the `asio::ssl::context` passed to the constructor. In cases when no `asio::ssl::context` was provided by the user, it uses a shared default `tls::system_context`.
- Builds without a TLS backend (no OpenSSL or wolfSSL), will get `tls::backend_error::unavailable` when trying to connect via `wss://`.


Websocket message `aero::websocket::message`:
```cpp
struct message {
  websocket::message_kind kind{};
  std::vector<std::byte> payload;

  [[nodiscard]] bool is_text() const noexcept;
  [[nodiscard]] bool is_binary() const noexcept;
  [[nodiscard]] bool is_close() const noexcept;
  [[nodiscard]] bool is_ping() const noexcept;
  [[nodiscard]] bool is_pong() const noexcept;
  [[nodiscard]] bool is_control() const noexcept;

  [[nodiscard]] bool has_payload() const noexcept;
  [[nodiscard]] std::string_view text() const noexcept;
  [[nodiscard]] std::span<const std::byte> bytes() const noexcept;

  [[nodiscard]] bool has_close_code() const noexcept;
  [[nodiscard]] std::optional<websocket::close_code> close_code() const noexcept;
  [[nodiscard]] bool has_close_reason() const noexcept;
  [[nodiscard]] std::optional<std::string_view> close_reason() const noexcept;
};
```

### Asynchronous API
Any asynchronous operation accepts a completion token from the asio world. This means you can use many different tokens from asio for any operation, and you are not limited to any form of asynchrony. Example of using completion tokens from asio:
```cpp
using namespace std::chrono_literals;

// Return result as an awaitable tuple (asio::awaitable<std::tuple>)
auto [connect_ec, handshake_response] =
  co_await client.async_connect("ws://example.com/", asio::as_tuple(asio::use_awaitable));

// Return result as an awaitable tuple with timeout of 1500ms
auto [read_ec, message] = co_await client.async_read(asio::cancel_after(1500ms, asio::as_tuple(asio::use_awaitable)));

// Capture the error separately and receive only the HTTP response from co_await
std::error_code redirected_connect_ec;
auto redirected_handshake_response =
  co_await client.async_connect("ws://example.com/", asio::redirect_error(asio::use_awaitable, redirected_connect_ec));

// Use functor with correct completion signature instead of coroutines
client.async_connect("ws://example.com/", [](std::error_code ec, aero::http::response response) {});

// Return 'std::future' from async operation
auto completion_future = client.async_connect("ws://example.com/", asio::use_future);
```

|Function|Completion Signature|
|-|-|
|`async_connect(...)`|void(std::error_code, aero::http::response)|
|`async_send_text(...)`|void(std::error_code)|
|`async_send_binary(...)`|void(std::error_code)|
|`async_ping(...)`|void(std::error_code)|
|`async_pong(...)`|void(std::error_code)|
|`async_close(...)`|void(std::error_code)|
|`async_force_close(...)`|void(std::error_code)|
|`async_read(...)`|void(std::error_code, aero::websocket::message)|

### Threadsafety
Please note that all references to functions apply to both synchronous and asynchronous variants. For example, if `async_connect` is mentioned, this implies all overloads of this function and its synchronous variant `connect`.

|Operation|Contract|
|-|-|
|`async_connect`|Allowed only on a closed connection. When called while the connection is open, connecting or closing, it returns `websocket::protocol_error::connection_not_closed` and leaves the current session untouched. When `async_connect` is called twice concurrently, the second call gets `connection_not_closed`. Any running `async_read`/`async_close` must have completed before reconnecting.|
|`async_send_text`, `async_send_binary`, `async_ping`, `async_pong`|Can be called concurrently with any operation except `async_connect`. The connection serializes all writes internally on its strand. A write issued before the connection is open returns `websocket::protocol_error::connection_closed`. Can be called concurrently with `async_close`, but the result depends on strand ordering - once in closing, it will return `websocket::protocol_error::connection_closed`.|
|`async_close`|Threadsafe, but correct usage is only one close at a time. A second concurrent call returns `websocket::protocol_error::already_closing`. Forbidden concurrently with `async_connect` (possible competing reads). Allowed to use with `async_read` concurrently, and if `async_close` starts first, it may start reading and an external `async_read` will return `websocket::protocol_error::already_reading`|
|`async_force_close`|Threadsafe, cancels all running operations|
|`is_open_for_writing`, `is_connecting`, `is_closed`, `is_closing`, `get_executor`|Threadsafe getters|

## HTTP

### What you get
- request and response types
- methods, status codes and versions as enums, not strings
- request line, status line and URL parsing
- header utilities that are actually pleasant to work with

# Build and integration
Aero is header‑only and requires a C++23 compiler. The primary dependency is standalone Asio (header‑only), and optional TLS support depends on WolfSSL or OpenSSL. Currently, Aero does not support package managers due to time constraints and because the current version is an MVP. PRs are very welcome.

You can include Aero in your project with CMake using `add_subdirectory` or `FetchContent`:
```cmake
cmake_minimum_required(VERSION 3.24)
project(my_app LANGUAGES CXX)

# Add aero as a subdirectory
add_subdirectory(path/to/aero)

add_executable(my_app main.cpp)
target_link_libraries(my_app PRIVATE aero)

# Optional settings (configure before the first configure step)
# You can find all of the options in 'cmake/AeroOptions.cmake'

# Use installed wolfssl target. Supported backends are: wolfssl|openssl|none.
# Default is 'none'
# set(AERO_TLS_BACKEND wolfssl)

# set(AERO_USE_BUNDLED_ASIO ON) # "Fetch ASIO using FetchContent if not found on system or in targets"
```

### Building with presets

CMakePresets.json provides the necessary general presets for working with TLS and non-TLS builds. Tests and examples, as well as the export of compile_commands.json, are included by default. Place custom settings, such as vcpkg triplets and other settings, in CMakeUserPresets.json.

### wolfSSL via vcpkg

Install the port with the `asio` feature. When building on Windows, you also need to pass the overlay triplets provided by this repository, since they enable the wolfSSL error queue, without which certificate loading fails.

```
vcpkg install "wolfssl[asio]" --overlay-triplets path/to/aero/cmake/vcpkg-triplets
```

See the TODO in `cmake/vcpkg-triplets/` for details.

## Tests
Actions build and test Aero on:
|Compiler|Platform|Modes|
|-|-|-|
|Clang (20+)|Linux|With TLS & No TLS|
|GCC (15.2+)|Linux|With TLS & No TLS|
|MSVC|Windows|With TLS & No TLS|
|Clang-CL|Windows|With TLS & No TLS|
|AppleClang|macOS|With TLS & No TLS|

Aero contains ~300+ unit tests of implementation details

The Websocket protocol is tested using [autobahn](https://github.com/crossbario/autobahn-testsuite) (a set of tests for RFC6455 compliance used by industry giants).

## License
Aero is distributed under the MIT License.
