#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <ut/ut.hpp>
#include <vector>

#include "aero/websocket/close_code.hpp"
#include "aero/websocket/detail/client_frame_builder.hpp"
#include "aero/websocket/detail/opcode.hpp"
#include "aero/websocket/error.hpp"

#include "websocket/test_helpers.hpp"

using namespace ut;

using aero::websocket::close_code;
using aero::websocket::detail::client_frame_builder;
using aero::websocket::detail::masking_key;
using aero::websocket::detail::opcode;

using aero::tests::websocket::big_endian_bytes;
using aero::tests::websocket::build_frame_bytes_canonical;
using aero::tests::websocket::make_masking_key;
using aero::tests::websocket::make_payload_bytes;
using aero::tests::websocket::starts_with;
using aero::tests::websocket::to_bytes;
using aero::tests::websocket::to_string;
using aero::tests::websocket::unmask_payload;

using protocol_error = aero::websocket::protocol_error;

masking_key extract_masking_key(std::span<const std::byte> frame_bytes) {
  const auto second = std::to_integer<std::uint8_t>(frame_bytes[1]);
  const std::uint8_t length_indicator = second & 0x7FU;

  std::size_t extended_length_size = 0;
  if (length_indicator == 126U) {
    extended_length_size = 2;
  } else if (length_indicator == 127U) {
    extended_length_size = 8;
  }

  const std::size_t key_offset = 2U + extended_length_size;

  return masking_key{
    frame_bytes[key_offset + 0U],
    frame_bytes[key_offset + 1U],
    frame_bytes[key_offset + 2U],
    frame_bytes[key_offset + 3U],
  };
}

std::span<const std::byte> payload_bytes(std::span<const std::byte> frame_bytes) {
  const auto second = std::to_integer<std::uint8_t>(frame_bytes[1]);
  const std::uint8_t length_indicator = second & 0x7FU;

  std::size_t payload_length = 0;
  std::size_t extended_length_size = 0;

  if (length_indicator <= 125U) {
    payload_length = length_indicator;
  } else if (length_indicator == 126U) {
    extended_length_size = 2;
    payload_length = (static_cast<std::size_t>(std::to_integer<std::uint8_t>(frame_bytes[2])) << 8U) |
                     static_cast<std::size_t>(std::to_integer<std::uint8_t>(frame_bytes[3]));
  } else {
    extended_length_size = 8;
    std::uint64_t value = 0;
    for (std::size_t i{}; i < 8U; ++i) {
      value = (value << 8U) | std::to_integer<std::uint8_t>(frame_bytes[2U + i]);
    }
    payload_length = static_cast<std::size_t>(value);
  }

  const std::size_t payload_offset = 2U + extended_length_size + 4U;
  return frame_bytes.subspan(payload_offset, payload_length);
}

std::vector<std::byte> mask_payload(std::span<const std::byte> unmasked_payload, masking_key key) {
  std::vector<std::byte> masked;
  masked.resize(unmasked_payload.size());
  for (std::size_t i{}; i < unmasked_payload.size(); ++i) {
    const auto value = std::to_integer<std::uint8_t>(unmasked_payload[i]);
    const auto key_value = std::to_integer<std::uint8_t>(key[i % 4U]);
    masked[i] = std::byte{static_cast<std::uint8_t>(value ^ key_value)};
  }
  return masked;
}

std::vector<std::byte> expected_masked_header_prefix(opcode frame_opcode, std::size_t payload_length, masking_key key) {
  const auto opcode_value = static_cast<std::uint8_t>(frame_opcode) & 0x0FU;
  std::span<const std::byte> empty_payload{};
  auto bytes = build_frame_bytes_canonical(true, false, false, false, opcode_value, true, payload_length, key, empty_payload);
  return bytes;
}

struct fixed_masking_key_source {
  masking_key key{};

  std::expected<masking_key, std::error_code> next() {
    return key;
  }
};

struct failing_masking_key_source {
  std::error_code error;

  std::expected<masking_key, std::error_code> next() {
    return std::unexpected(error);
  }
};

struct sequence_masking_key_source {
  std::vector<masking_key> keys;
  std::size_t index{0};

  std::expected<masking_key, std::error_code> next() {
    if (index >= keys.size()) {
      return std::unexpected(protocol_error::masking_key_generation_failed);
    }
    return keys[index++];
  }
};

int main() {
  suite websocket_client_frame_builder = [] {
    "builds masked final text frame with zeroed rsv bits"_test = [] {
      const masking_key key = make_masking_key(0x00, 0x01, 0x02, 0x03);
      client_frame_builder<fixed_masking_key_source> builder{fixed_masking_key_source{key}};

      auto built_text_frame = builder.build_text_frame("Hi");
      require(built_text_frame.has_value());

      auto text_frame = built_text_frame.value();

      const auto expected_prefix = expected_masked_header_prefix(opcode::text, 2U, key);
      expect(starts_with(text_frame, expected_prefix));

      expect(text_frame.size() == expected_prefix.size() + 2U);

      const auto first = std::to_integer<std::uint8_t>(text_frame[0]);
      const auto first_fin_bit = first & 0x80U;
      const auto first_rsv_bits = first & 0x70U;
      const auto first_opcode_bits = first & 0x0FU;
      const auto expected_opcode_bits = static_cast<std::uint8_t>(opcode::text) & 0x0FU;

      expect(first_fin_bit == 0x80U);
      expect(first_rsv_bits == 0x00U);
      expect(first_opcode_bits == expected_opcode_bits);

      const auto second = std::to_integer<std::uint8_t>(text_frame[1]);
      const auto second_mask_bit = second & 0x80U;
      expect(second_mask_bit == 0x80U);
    };

    "masks text payload using provided masking key"_test = [] {
      const masking_key key = make_masking_key(0x12, 0x34, 0x56, 0x78);
      client_frame_builder<fixed_masking_key_source> builder{fixed_masking_key_source{key}};

      const auto payload = to_bytes("abcd");
      auto built_text_frame = builder.build_text_frame("abcd");
      require(built_text_frame.has_value());

      auto text_frame = built_text_frame.value();

      const auto header_prefix = expected_masked_header_prefix(opcode::text, payload.size(), key);
      expect(starts_with(text_frame, header_prefix));

      const auto extracted_key = extract_masking_key(text_frame);
      expect(extracted_key == key);

      const auto masked_payload = payload_bytes(text_frame);
      const auto expected_masked = mask_payload(payload, key);
      require(masked_payload.size() == expected_masked.size());
      for (std::size_t i{}; i < expected_masked.size(); ++i) {
        expect(masked_payload[i] == expected_masked[i]);
      }
    };

    "encodes masked payload length using 7-bit encoding for small payloads"_test = [] {
      const masking_key key = make_masking_key(0xAA, 0xBB, 0xCC, 0xDD);
      client_frame_builder<fixed_masking_key_source> builder{fixed_masking_key_source{key}};

      std::string payload(125, 'x');
      auto built_text_frame = builder.build_text_frame(payload);
      require(built_text_frame.has_value());

      auto text_frame = built_text_frame.value();

      const auto expected_prefix = expected_masked_header_prefix(opcode::text, 125U, key);
      expect(starts_with(text_frame, expected_prefix));
      expect(text_frame.size() == expected_prefix.size() + 125U);

      const auto second = std::to_integer<std::uint8_t>(text_frame[1]);
      expect(second == static_cast<std::uint8_t>(0x80U | 125U));
    };

    "encodes masked payload length using 16-bit extended encoding from 126 to 65535"_test = [] {
      const masking_key key = make_masking_key(0x00, 0x00, 0x00, 0x01);
      client_frame_builder<fixed_masking_key_source> builder{fixed_masking_key_source{key}};

      std::string payload(126, 'y');
      auto built_text_frame = builder.build_text_frame(payload);
      require(built_text_frame.has_value());

      auto text_frame = built_text_frame.value();

      const auto expected_prefix = expected_masked_header_prefix(opcode::text, 126U, key);
      expect(starts_with(text_frame, expected_prefix));
      expect(text_frame.size() == expected_prefix.size() + 126U);

      const auto second = std::to_integer<std::uint8_t>(text_frame[1]);
      expect(second == static_cast<std::uint8_t>(0x80U | 126U));

      const auto extended = big_endian_bytes<2>(126U);
      expect(text_frame[2] == extended[0]);
      expect(text_frame[3] == extended[1]);
    };

    "encodes masked payload length using 64-bit extended encoding from 65536 upwards"_test = [] {
      const masking_key key = make_masking_key(0x10, 0x11, 0x12, 0x13);
      client_frame_builder<fixed_masking_key_source> builder{fixed_masking_key_source{key}};

      std::string payload(65536, 'z');
      auto built_text_frame = builder.build_text_frame(payload);
      require(built_text_frame.has_value());

      auto text_frame = built_text_frame.value();

      const auto expected_prefix = expected_masked_header_prefix(opcode::text, 65536U, key);
      expect(starts_with(text_frame, expected_prefix));
      expect(text_frame.size() == expected_prefix.size() + 65536U);

      const auto second = std::to_integer<std::uint8_t>(text_frame[1]);
      expect(second == static_cast<std::uint8_t>(0x80U | 127U));

      const auto extended = big_endian_bytes<8>(65536U);
      for (std::size_t i{}; i < 8U; ++i) {
        expect(text_frame[2U + i] == extended[i]);
      }
    };

    "builds masked binary frame and masks payload bytes"_test = [] {
      const masking_key key = make_masking_key(0x01, 0x02, 0x03, 0x04);
      client_frame_builder<fixed_masking_key_source> builder{fixed_masking_key_source{key}};

      std::vector<std::byte> payload{std::byte{0x00}, std::byte{0xFF}, std::byte{0x10}, std::byte{0x20}, std::byte{0x30}};
      auto built_binary_frame = builder.build_binary_frame(payload);
      require(built_binary_frame.has_value());

      auto binary_frame = built_binary_frame.value();

      const auto expected_prefix = expected_masked_header_prefix(opcode::binary, payload.size(), key);
      expect(starts_with(binary_frame, expected_prefix));
      expect(binary_frame.size() == expected_prefix.size() + payload.size());

      const auto masked = payload_bytes(binary_frame);
      const auto expected_masked = mask_payload(payload, key);

      for (std::size_t i{}; i < payload.size(); ++i) {
        expect(masked[i] == expected_masked[i]);
      }
    };

    "requests new masking key for each frame"_test = [] {
      sequence_masking_key_source source;
      source.keys.push_back(make_masking_key(0x00, 0x00, 0x00, 0x00));
      source.keys.push_back(make_masking_key(0x01, 0x01, 0x01, 0x01));
      client_frame_builder<sequence_masking_key_source> builder{source};

      auto built_first_frame = builder.build_text_frame("a");
      require(built_first_frame.has_value());

      auto built_second_frame = builder.build_text_frame("b");
      require(built_second_frame.has_value());

      const auto first_key = extract_masking_key(built_first_frame.value());
      const auto second_key = extract_masking_key(built_second_frame.value());

      expect(first_key != second_key);
    };

    "propagates masking key source failure"_test = [] {
      failing_masking_key_source source;
      source.error = protocol_error::masking_key_generation_failed;

      client_frame_builder<failing_masking_key_source> builder{source};
      auto built_text_frame = builder.build_text_frame("x");

      require(not built_text_frame.has_value());
      expect(built_text_frame.error() == protocol_error::masking_key_generation_failed);
    };

    "builds ping frame as final masked control frame"_test = [] {
      const masking_key key = make_masking_key(0xAB, 0xCD, 0xEF, 0x01);
      client_frame_builder<fixed_masking_key_source> builder{fixed_masking_key_source{key}};

      auto payload = to_bytes("p");
      auto built_ping_frame = builder.build_ping_frame(std::span<const std::byte>{payload});
      require(built_ping_frame.has_value());

      auto ping_frame = built_ping_frame.value();

      const auto expected_prefix = expected_masked_header_prefix(opcode::ping, 1U, key);
      expect(starts_with(ping_frame, expected_prefix));
      expect(ping_frame.size() == expected_prefix.size() + 1U);

      const auto first = std::to_integer<std::uint8_t>(ping_frame[0]);
      const auto fin_bit = first & 0x80U;
      const auto rsv_bits = first & 0x70U;
      const auto opcode_bits = first & 0x0FU;
      const auto expected_opcode_bits = static_cast<std::uint8_t>(opcode::ping) & 0x0FU;
      expect(fin_bit == 0x80U);
      expect(rsv_bits == 0x00U);
      expect(opcode_bits == expected_opcode_bits);
    };

    "rejects ping payload longer than 125 bytes"_test = [] {
      const masking_key key = make_masking_key(0x00, 0x01, 0x02, 0x03);
      client_frame_builder<fixed_masking_key_source> builder{fixed_masking_key_source{key}};

      auto payload = make_payload_bytes(126, std::byte{0x11});
      auto built_ping_frame = builder.build_ping_frame(std::span<const std::byte>{payload});

      require(not built_ping_frame.has_value());
      expect(built_ping_frame.error() == protocol_error::control_frame_payload_too_big);
    };

    "rejects pong payload longer than 125 bytes with control frame error"_test = [] {
      const masking_key key = make_masking_key(0x00, 0x01, 0x02, 0x03);
      client_frame_builder<fixed_masking_key_source> builder{fixed_masking_key_source{key}};

      auto payload = make_payload_bytes(126, std::byte{0x22});
      auto built_pong_frame = builder.build_pong_frame(std::span<const std::byte>{payload});

      require(not built_pong_frame.has_value());
      expect(built_pong_frame.error() == protocol_error::control_frame_payload_too_big);
    };

    "builds pong frame as final masked control frame"_test = [] {
      const masking_key key = make_masking_key(0x10, 0x20, 0x30, 0x40);
      client_frame_builder<fixed_masking_key_source> builder{fixed_masking_key_source{key}};

      auto payload = make_payload_bytes(125, std::byte{0x33});
      auto built_pong_frame = builder.build_pong_frame(std::span<const std::byte>{payload});
      require(built_pong_frame.has_value());

      auto pong_frame = built_pong_frame.value();

      const auto expected_prefix = expected_masked_header_prefix(opcode::pong, 125U, key);
      expect(starts_with(pong_frame, expected_prefix));
      expect(pong_frame.size() == expected_prefix.size() + 125U);

      const auto first = std::to_integer<std::uint8_t>(pong_frame[0]);
      const auto fin_bit = first & 0x80U;
      const auto opcode_bits = first & 0x0FU;
      const auto expected_opcode_bits = static_cast<std::uint8_t>(opcode::pong) & 0x0FU;
      expect(fin_bit == 0x80U);
      expect(opcode_bits == expected_opcode_bits);
    };

    "builds close frame with status code in network byte order"_test = [] {
      const masking_key key = make_masking_key(0x55, 0x66, 0x77, 0x88);
      client_frame_builder<fixed_masking_key_source> builder{fixed_masking_key_source{key}};

      auto built_close_frame = builder.build_close_frame(close_code::normal, std::nullopt);
      require(built_close_frame.has_value());

      auto close_frame = built_close_frame.value();

      const auto expected_prefix = expected_masked_header_prefix(opcode::close, 2U, key);
      expect(starts_with(close_frame, expected_prefix));
      expect(close_frame.size() == expected_prefix.size() + 2U);

      const auto masked = payload_bytes(close_frame);
      const auto unmasked = unmask_payload(masked, key);

      const auto expected_code = big_endian_bytes<2>(std::to_underlying(close_code::normal));
      require(unmasked.size() == 2U);
      expect(unmasked[0] == expected_code[0]);
      expect(unmasked[1] == expected_code[1]);
    };

    "builds close frame with reason without padding or duplication"_test = [] {
      const masking_key key = make_masking_key(0x01, 0x23, 0x45, 0x67);
      client_frame_builder<fixed_masking_key_source> builder{fixed_masking_key_source{key}};

      constexpr std::string_view reason = "bye";
      auto built_close_frame = builder.build_close_frame(close_code::normal, reason);
      require(built_close_frame.has_value());

      auto close_frame = built_close_frame.value();

      constexpr auto payload_length = 2U + reason.size();
      const auto expected_prefix = expected_masked_header_prefix(opcode::close, payload_length, key);
      expect(starts_with(close_frame, expected_prefix));
      expect(close_frame.size() == expected_prefix.size() + payload_length);

      const auto unmasked = unmask_payload(payload_bytes(close_frame), key);
      require(unmasked.size() == payload_length);
      const auto expected_code = big_endian_bytes<2>(std::to_underlying(close_code::normal));
      expect(unmasked[0] == expected_code[0]);
      expect(unmasked[1] == expected_code[1]);

      const std::string recovered_reason = to_string(std::span<const std::byte>{unmasked}.subspan(2));
      expect(recovered_reason == reason);
    };

    "rejects close reason that is not valid utf8"_test = [] {
      const masking_key key = make_masking_key(0x99, 0x88, 0x77, 0x66);
      client_frame_builder<fixed_masking_key_source> builder{fixed_masking_key_source{key}};

      std::string invalid_utf8;
      invalid_utf8.push_back(static_cast<char>(0xC3));
      invalid_utf8.push_back(static_cast<char>(0x28));

      auto built_close_frame = builder.build_close_frame(close_code::normal, invalid_utf8);
      require(not built_close_frame.has_value());
      expect(built_close_frame.error() == protocol_error::close_reason_invalid_utf8);
    };

    "rejects text payload that is not valid utf8"_test = [] {
      const masking_key key = make_masking_key(0x01, 0x02, 0x03, 0x04);
      client_frame_builder<fixed_masking_key_source> builder{fixed_masking_key_source{key}};

      std::string invalid_utf8;
      invalid_utf8.push_back(static_cast<char>(0xC3));
      invalid_utf8.push_back(static_cast<char>(0x28));

      auto built_text_frame = builder.build_text_frame(invalid_utf8);
      require(not built_text_frame.has_value());
      expect(built_text_frame.error() == protocol_error::payload_text_invalid_utf8);
    };

    "ignores invalid utf8 when validation is disabled via config"_test = [] {
      const masking_key key = make_masking_key(0x99, 0x88, 0x77, 0x66);
      client_frame_builder<fixed_masking_key_source> builder{fixed_masking_key_source{key}, {.validate_utf8 = false}};

      std::string invalid_utf8;
      invalid_utf8.push_back(static_cast<char>(0xC3));
      invalid_utf8.push_back(static_cast<char>(0x28));

      auto built_frame = builder.build_close_frame(close_code::normal, invalid_utf8);
      expect(built_frame.has_value());

      built_frame = builder.build_text_frame(invalid_utf8);
      expect(built_frame.has_value());
    };

    "accepts maximum allowed close reason length for control frame payload limit"_test = [] {
      const masking_key key = make_masking_key(0x01, 0x02, 0x03, 0x04);
      client_frame_builder<fixed_masking_key_source> builder{fixed_masking_key_source{key}};

      std::string reason(123, 'r');
      auto built_close_frame = builder.build_close_frame(close_code::normal, reason);
      require(built_close_frame.has_value());

      auto close_frame = built_close_frame.value();

      const std::size_t payload_length = 125U;
      const auto expected_prefix = expected_masked_header_prefix(opcode::close, payload_length, key);
      expect(starts_with(close_frame, expected_prefix));
      expect(close_frame.size() == expected_prefix.size() + payload_length);
    };

    "rejects close reason that would exceed control frame payload limit"_test = [] {
      const masking_key key = make_masking_key(0x01, 0x02, 0x03, 0x04);
      client_frame_builder<fixed_masking_key_source> builder{fixed_masking_key_source{key}};

      std::string reason(124, 'r');
      auto built_close_frame = builder.build_close_frame(close_code::normal, reason);
      require(not built_close_frame.has_value());
      expect(built_close_frame.error() == protocol_error::control_frame_payload_too_big);
    };
  };
}
