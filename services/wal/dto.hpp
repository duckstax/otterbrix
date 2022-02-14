#pragma once

#include <cstdint>
#include <msgpack.hpp>
#include <vector>

using buffer_element_t = char;
using buffer_t = std::vector<buffer_element_t>;

using size_tt = std::uint16_t;
using log_number_t = std::uint64_t;
using crc32_t = std::uint32_t;

struct entry_t final {
    entry_t() = default;
    entry_t(crc32_t last_crc32, log_number_t logNumber)
        : last_crc32_(last_crc32)
        , log_number_(logNumber) {}

    crc32_t last_crc32_;
    log_number_t log_number_;
};

struct wal_entry_t final {
    size_tt size_;
    entry_t entry_;
    crc32_t crc32_;
};

crc32_t pack(buffer_t& storage, char* data, size_t size);

template<class T>
crc32_t pack(buffer_t& storage, crc32_t last_crc32, log_number_t log_number, T& data) {
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(buffer);

    packer.pack_array(3 + data.size());
    packer.pack_fix_uint32(last_crc32);
    packer.pack_fix_uint64(log_number);
    pack(packer, data);

    return pack(storage, buffer.data(), buffer.size());
}

crc32_t pack(buffer_t& storage, entry_t&);

void unpack(buffer_t& storage, wal_entry_t&);

void unpack_v2(buffer_t& storage, wal_entry_t&);