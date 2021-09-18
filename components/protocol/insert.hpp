#pragma once

#include "base.hpp"
#include "request_select.hpp"
#include <msgpack.hpp>
#include <string>
#include <vector>

struct insert_t final {
    insert_t() = default;
    insert_t(std::string&& name_table, std::vector<std::string>&& column_name, std::vector<std::string>&& values);
    std::string name_table_;
    std::vector<std::string> column_name_;
    std::vector<std::string> values_;
};

namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<insert_t> final {
                msgpack::object const& operator()(msgpack::object const& o, insert_t& v) const {
                    if (o.type != msgpack::type::ARRAY) {
                        throw msgpack::type_error();
                    }

                    if (o.via.array.size != 3) {
                        throw msgpack::type_error();
                    }

                    v = insert_t(
                        o.via.array.ptr[0].as<std::string>()
                            ,o.via.array.ptr[1].as<std::vector<std::string>>()
                            ,o.via.array.ptr[2].as<std::vector<std::string>>());
                    return o;
                }
            };

            template<>
            struct pack<insert_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, insert_t const& v) const {
                    o.pack_array(3);
                    o.pack_str_body(v.name_table_.data(), v.name_table_.size());
                    o.pack_array(v.column_name_.size());
                    for (const auto& i : v.column_name_) {
                        msgpack::pack(o, i);
                    }
                    o.pack_array(v.values_.size());
                    for (const auto& i : v.values_) {
                        msgpack::pack(o, i);
                    }
                    return o;
                }
            };

            template<>
            struct object_with_zone<insert_t> final {
                void operator()(msgpack::object::with_zone& o, insert_t const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 2;
                    o.via.array.ptr = static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size, MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v.name_table_, o.zone);
                    o.via.array.ptr[1] = msgpack::object(v.column_name_, o.zone);
                    o.via.array.ptr[1] = msgpack::object(v.values_, o.zone);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack