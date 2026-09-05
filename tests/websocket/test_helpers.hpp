#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "aero/websocket/close_code.hpp"
#include "aero/websocket/detail/frame.hpp"
#include "aero/websocket/detail/opcode.hpp"

namespace aero::tests::websocket {

  using aero::websocket::close_code;
  using aero::websocket::detail::frame;
  using aero::websocket::detail::masking_key;
  using aero::websocket::detail::opcode;

  inline std::byte to_byte(std::uint8_t value) {
    return static_cast<std::byte>(value);
  }

  template <std::size_t ByteCount>
  inline std::array<std::byte, ByteCount> big_endian_bytes(std::uint64_t value) {
    std::array<std::byte, ByteCount> out{};
    for (std::size_t i{}; i < ByteCount; ++i) {
      const auto shift = (ByteCount - 1U - i) * 8U;
      out[i] = to_byte(static_cast<std::uint8_t>((value >> shift) & 0xFFU));
    }
    return out;
  }

  inline std::vector<std::byte> to_bytes(std::string_view text) {
    std::vector<std::byte> bytes;
    bytes.reserve(text.size());
    for (char character : text) {
      bytes.push_back(static_cast<std::byte>(character));
    }
    return bytes;
  }

  inline std::string to_string(std::span<const std::byte> bytes) {
    std::string text;
    text.resize(bytes.size());
    for (std::size_t i{}; i < bytes.size(); ++i) {
      text[i] = static_cast<char>(std::to_integer<unsigned char>(bytes[i]));
    }
    return text;
  }

  inline std::vector<std::byte> make_payload_bytes(std::size_t size, std::byte fill = std::byte{0}) {
    return {size, fill};
  }

  inline masking_key make_masking_key(std::uint8_t first, std::uint8_t second, std::uint8_t third, std::uint8_t fourth) {
    return masking_key{
      static_cast<std::byte>(first),
      static_cast<std::byte>(second),
      static_cast<std::byte>(third),
      static_cast<std::byte>(fourth),
    };
  }

  inline std::vector<std::byte> unmask_payload(std::span<const std::byte> masked_payload, masking_key key) {
    std::vector<std::byte> unmasked;
    unmasked.resize(masked_payload.size());
    for (std::size_t i{}; i < masked_payload.size(); ++i) {
      const auto masked_value = std::to_integer<std::uint8_t>(masked_payload[i]);
      const auto key_value = std::to_integer<std::uint8_t>(key[i % 4U]);
      unmasked[i] = std::byte{static_cast<std::uint8_t>(masked_value ^ key_value)};
    }
    return unmasked;
  }

  inline std::array<std::byte, 2> make_network_u16(std::uint16_t value) {
    return big_endian_bytes<2>(value);
  }

  inline std::byte make_first_byte(bool fin, bool rsv1, bool rsv2, bool rsv3, std::uint8_t opcode_value) {
    const std::uint32_t fin_bits = fin ? 0x80U : 0x00U;
    const std::uint32_t rsv1_bits = rsv1 ? 0x40U : 0x00U;
    const std::uint32_t rsv2_bits = rsv2 ? 0x20U : 0x00U;
    const std::uint32_t rsv3_bits = rsv3 ? 0x10U : 0x00U;
    const std::uint32_t opcode_bits = static_cast<std::uint32_t>(opcode_value) & 0x0FU;
    return to_byte(static_cast<std::uint8_t>(fin_bits | rsv1_bits | rsv2_bits | rsv3_bits | opcode_bits));
  }

  inline std::byte make_second_byte(bool masked, std::uint8_t payload_length_indicator) {
    const std::uint8_t mask_bit = 0x80U;
    const std::uint8_t masking_bits = masked ? mask_bit : 0x00U;
    return to_byte(static_cast<std::uint8_t>(masking_bits | (payload_length_indicator & 0x7FU)));
  }

  inline std::vector<std::byte> build_frame_bytes_explicit(bool fin, bool rsv1, bool rsv2, bool rsv3, std::uint8_t opcode_value,
    bool masked, std::uint8_t payload_length_indicator, std::span<const std::byte> extended_length_bytes,
    std::optional<masking_key> key, std::span<const std::byte> payload_bytes) {
    std::vector<std::byte> bytes;
    bytes.reserve(2U + extended_length_bytes.size() + (masked ? 4U : 0U) + payload_bytes.size());
    bytes.push_back(make_first_byte(fin, rsv1, rsv2, rsv3, opcode_value));
    bytes.push_back(make_second_byte(masked, payload_length_indicator));
    bytes.insert(bytes.end(), extended_length_bytes.begin(), extended_length_bytes.end());
    if (masked) {
      const auto masking_key_value = key.value_or(masking_key{std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}});
      bytes.insert(bytes.end(), masking_key_value.begin(), masking_key_value.end());
    }
    bytes.insert(bytes.end(), payload_bytes.begin(), payload_bytes.end());
    return bytes;
  }

  inline std::vector<std::byte> build_frame_bytes_canonical(bool fin, bool rsv1, bool rsv2, bool rsv3,
    std::uint8_t opcode_value, bool masked, std::uint64_t payload_length, std::optional<masking_key> key,
    std::span<const std::byte> payload_bytes) {
    constexpr std::uint8_t payload_len_7_max = 125U;
    constexpr std::uint8_t payload_len_16_indicator = 126U;
    constexpr std::uint8_t payload_len_64_indicator = 127U;

    if (payload_length <= payload_len_7_max) {
      return build_frame_bytes_explicit(fin,
        rsv1,
        rsv2,
        rsv3,
        opcode_value,
        masked,
        static_cast<std::uint8_t>(payload_length),
        {},
        key,
        payload_bytes);
    }

    if (payload_length <= 65535U) {
      const auto extended = big_endian_bytes<2>(payload_length);
      return build_frame_bytes_explicit(fin,
        rsv1,
        rsv2,
        rsv3,
        opcode_value,
        masked,
        payload_len_16_indicator,
        extended,
        key,
        payload_bytes);
    }

    const auto extended = big_endian_bytes<8>(payload_length);
    return build_frame_bytes_explicit(fin,
      rsv1,
      rsv2,
      rsv3,
      opcode_value,
      masked,
      payload_len_64_indicator,
      extended,
      key,
      payload_bytes);
  }

  inline std::vector<std::byte> serialize_unmasked_frame(opcode frame_opcode, bool is_final,
    std::span<const std::byte> payload) {
    const auto opcode_value = static_cast<std::uint8_t>(frame_opcode) & 0x0FU;
    return build_frame_bytes_canonical(is_final,
      false,
      false,
      false,
      opcode_value,
      false,
      payload.size(),
      std::nullopt,
      payload);
  }

  inline std::vector<std::byte> serialize_close_payload(close_code code, std::span<const std::byte> reason) {
    auto close_code_value = std::to_underlying(code);

    std::vector<std::byte> payload;
    payload.reserve(2U + reason.size());
    payload.push_back(std::byte{static_cast<std::uint8_t>((close_code_value >> 8U) & 0xFFU)});
    payload.push_back(std::byte{static_cast<std::uint8_t>(close_code_value & 0xFFU)});
    payload.append_range(reason);
    return payload;
  }

  inline frame make_minimal_frame(opcode frame_opcode) {
    return frame{
      .fin = true,
      .rsv1 = false,
      .rsv2 = false,
      .rsv3 = false,
      .opcode = frame_opcode,
      .masked = false,
      .payload_length = 0,
      .masking_key = std::nullopt,
      .payload_data = std::span<const std::byte>{},
      .extension_data = std::span<const std::byte>{},
      .application_data = std::span<const std::byte>{},
    };
  }

  inline bool starts_with(std::span<const std::byte> bytes, std::span<const std::byte> prefix) {
    if (prefix.size() > bytes.size()) {
      return false;
    }
    // std::ranges::starts_with is still not implemented by libstdc++ 15.2
    return std::ranges::equal(bytes.subspan(0, prefix.size()), prefix);
  }

} // namespace aero::tests::websocket
