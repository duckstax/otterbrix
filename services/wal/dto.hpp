#pragma once

#include <cstdint>
#include <msgpack.hpp>
#include <vector>
#include "components/protocol/insert_many.hpp"

using buffer_element_t = char;
using buffer_t = std::vector<buffer_element_t>;

using size_tt = std::uint16_t;
using log_number_t = std::uint64_t;
using crc32_t = std::uint32_t;
/*
struct entry_t final {
    entry_t() = default;
    entry_t(crc32_t last_crc32, log_number_t logNumber)
        : last_crc32_(last_crc32)
        , log_number_(logNumber) {}

    crc32_t last_crc32_;
    log_number_t log_number_;
};
*/

template<class T>
struct wal_entry_t final {
    size_tt size_;
    T entry_;
    crc32_t crc32_;
};

crc32_t pack(buffer_t& storage, char* data, size_t size);

template<class T>
crc32_t pack(buffer_t& storage, crc32_t last_crc32, log_number_t log_number, T& data) {
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(buffer);

    packer.pack_array(4);
    packer.pack_fix_uint32(last_crc32);
    packer.pack_fix_uint64(log_number);
    packer.pack_char(static_cast<char>(data.type()));
    packer.pack(data);

    return pack(storage, buffer.data(), buffer.size());
}

template<class T>
void  unpack_v3(buffer_t& storage,wal_entry_t<T>& entry){
    auto start = 0;
    auto finish = entry.size_;
    auto buffer = read_payload(storage, start, finish);
    msgpack::unpacked msg;
    msgpack::unpack(msg, buffer.data(), buffer.size());
    const auto& o = msg.get();
    entry.entry_ = o.as<T>();
    entry.crc32_ = read_crc32(storage, entry.size_);
}
/*
crc32_t pack(buffer_t& storage, entry_t&);

void unpack(buffer_t& storage, wal_entry_t&);

void unpack_v2(buffer_t& storage, wal_entry_t&);
 */