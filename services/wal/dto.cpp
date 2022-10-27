#include "dto.hpp"

#include <unistd.h>
#include <chrono>
#include <crc32c/crc32c.h>
#include <msgpack.hpp>

namespace services::wal {

    void append_crc32(buffer_t& storage, crc32_t crc32) {
        storage.push_back(buffer_element_t(crc32 >> 24 & 0xff));
        storage.push_back(buffer_element_t(crc32 >> 16 & 0xff));
        storage.push_back(buffer_element_t(crc32 >> 8 & 0xff));
        storage.push_back(buffer_element_t(crc32 & 0xff));
    }

    void append_size(buffer_t& storage, size_tt size) {
        storage.push_back((buffer_element_t((size >> 8 & 0xff))));
        storage.push_back(buffer_element_t(size & 0xff));
    }

    void append_payload(buffer_t& storage, char* ptr, size_t size) {
        storage.insert(std::end(storage), ptr, ptr + size);
    }

    crc32_t read_crc32(buffer_t& input, int index_start) {
        crc32_t crc32_tmp = 0;
        crc32_tmp = 0xff000000 & (uint32_t(input[size_t(index_start)]) << 24);
        crc32_tmp |= 0x00ff0000 & (uint32_t(input[size_t(index_start) + 1]) << 16);
        crc32_tmp |= 0x0000ff00 & (uint32_t(input[size_t(index_start) + 2]) << 8);
        crc32_tmp |= 0x000000ff & (uint32_t(input[size_t(index_start) + 3]));
        return crc32_tmp;
    }

    buffer_t read_payload(buffer_t& input, int index_start, int index_stop) {
        buffer_t buffer(input.begin() + index_start, input.begin() + index_stop);
        return buffer;
    }

    size_tt read_size_impl(char* input, int index_start) {
        size_tt size_tmp = 0;
        size_tmp = 0xff00 & (size_tt(input[index_start] << 8));
        size_tmp |= 0x00ff & (size_tt(input[index_start + 1]));
        return size_tmp;
    }

    crc32_t pack(buffer_t& storage, char* input, size_t data_size) {
        auto last_crc32_ = crc32c::Crc32c(input, data_size);
        append_size(storage, size_tt(data_size));
        append_payload(storage, input, data_size);
        append_crc32(storage, last_crc32_);
        return last_crc32_;
    }

    id_t unpack_wal_id(buffer_t& storage) {
        msgpack::unpacked msg;
        msgpack::unpack(msg, storage.data(), storage.size());
        const auto& o = msg.get();
        return o.via.array.ptr[1].as<id_t>();
    }

} //namespace services::wal
