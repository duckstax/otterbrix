#include "dto.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <ctime>

#include <utility>
#include <chrono>

#include <crc32c/crc32c.h>

#include <msgpack.hpp>
#include <msgpack/adaptor/vector.hpp>


void append_crc32(buffer_t& storage, crc32_t crc32) {
    storage.push_back(buffer_element_t(crc32 >> 24 & 0xff));
    storage.push_back((buffer_element_t(crc32 >> 16 & 0xff)));
    storage.push_back((buffer_element_t(crc32 >> 8 & 0xff)));
    storage.push_back((buffer_element_t(crc32 & 0xff)));
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
    crc32_tmp = 0xff000000 & (uint32_t(input[index_start]) << 24);
    crc32_tmp |= 0x00ff0000 & (uint32_t(input[index_start + 1]) << 16);
    crc32_tmp |= 0x0000ff00 & (uint32_t(input[index_start + 2]) << 8);
    crc32_tmp |= 0x000000ff & (uint32_t(input[index_start + 3]));
    return crc32_tmp;
}

buffer_t read_payload(buffer_t& input, int index_start, int index_stop) {
    buffer_t buffer(input.begin() + index_start, input.begin() + index_stop);
    return buffer;
}
/*
size_tt read_size_impl(buffer_t& input, int index_start) {
    size_tt size_tmp = 0;
    size_tmp = 0xff00 & size_tt(input[index_start] << 8);
    size_tmp |= 0x00ff & size_tt(input[index_start + 1]);
    return size_tmp;
}
 */

size_tt read_size_impl(char* input, int index_start) {
    size_tt size_tmp = 0;
    size_tmp = 0xff00 & (size_tt(input[index_start] << 8));
    size_tmp |= 0x00ff & (size_tt(input[index_start + 1]));
    return size_tmp;
}
/*
// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<entry_t> final {
                msgpack::object const& operator()(msgpack::object const& o, entry_t& v) const {
                    if (o.type != msgpack::type::ARRAY) {
                        throw msgpack::type_error();
                    }

                    if (o.via.array.size != 4) {
                        throw msgpack::type_error();
                    }

                    auto last_crc32_ = o.via.array.ptr[0].as<crc32_t>();
                    auto type = o.via.array.ptr[1].as<uint8_t>();
                    auto log_number_ = o.via.array.ptr[2].as<log_number_t>();
                    auto buffer = o.via.array.ptr[3].as<buffer_t>();
                    v = entry_t(last_crc32_, static_cast<statement_type>(type), log_number_, std::move(buffer));
                    return o;
                }
            };

            template<>
            struct pack<entry_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, entry_t const& v) const {
                    o.pack_array(4);
                    o.pack_fix_uint32(v.last_crc32_);
                    o.pack_fix_uint64(v.log_number_);
                    o.pack_char(v.statement_packet_.type_);
                    switch (v.statement_packet_.type_) {
                        case statement_type::insert_many:{
                            auto data = get<insert_many_t>(v.statement_packet_.statement_);
                            o.pack(data);
                            break;
                        }

                    }
                    return o;
                }
            };

            template<>
            struct object_with_zone<entry_t> final {
                void operator()(msgpack::object::with_zone& o, entry_t const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 4;
                    o.via.array.ptr = static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size, MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v.last_crc32_, o.zone);
                    o.via.array.ptr[1] = msgpack::object(static_cast<uint8_t>(v.type_), o.zone);
                    o.via.array.ptr[2] = msgpack::object(v.log_number_, o.zone);
                    o.via.array.ptr[3] = msgpack::object(v.statement_packet_, o.zone);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
*/
/*
crc32_t pack(buffer_t& storage, entry_t& entry) {
    msgpack::sbuffer input;
    msgpack::pack(input, entry);
    auto last_crc32_ = crc32c::Crc32c(input.data(), input.size());
    append_size(storage, input.size());
    append_payload(storage, input.data(), input.size());
    append_crc32(storage, last_crc32_);
    return last_crc32_;
}

void unpack(buffer_t& storage, wal_entry_t& entry) {
    entry.size_ = read_size_impl(storage, 0);
    auto start = sizeof(size_tt);
    auto finish = sizeof(size_tt) + entry.size_;
    auto buffer = read_payload(storage, start, finish);
    msgpack::unpacked msg;
    msgpack::unpack(msg, buffer.data(), buffer.size());
    const auto& o = msg.get();
    entry.entry_ = o.as<entry_t>();
    auto crc32_index = sizeof(size_tt) + entry.size_;
    entry.crc32_ = read_crc32(storage, crc32_index);
}

void unpack_v2(buffer_t& storage, wal_entry_t& entry) {
    auto start = 0;
    auto finish = entry.size_;
    auto buffer = read_payload(storage, start, finish);
    msgpack::unpacked msg;
    msgpack::unpack(msg, buffer.data(), buffer.size());
    const auto& o = msg.get();
    entry.entry_ = o.as<entry_t>();
    entry.crc32_ = read_crc32(storage, entry.size_);
}
*/
crc32_t pack(buffer_t& storage, char* input, size_t data_size) {
    auto last_crc32_ = crc32c::Crc32c(input, data_size);
    append_size(storage, data_size);
    append_payload(storage, input, data_size);
    append_crc32(storage, last_crc32_);
    return last_crc32_;
}
