#pragma once
#include <cstdio>
#include <cstdint>

#include "dispatcher/route.hpp"

/**
 * header:
 * +---------+-----------+-----------+----------------+--- ... ---+
 * |CRC (4B) | Size (2B) | Type (1B) | Log number (8B)| Payload   |
 * +---------+-----------+-----------+----------------+--- ... ---+
 */


using size_tt = std::uint16_t;
using log_number_t = std::uint64_t;
using crc32_t = std::uint32_t;


constexpr  int heer_size = sizeof(crc32_t) + sizeof(size_tt) + sizeof(Type) + sizeof(log_number_t);
