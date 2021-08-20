#pragma once
#include <msgpack.hpp>

struct select_t {
    select_t() = default;
    select_t(const std::string& nameTable, const std::vector<std::string>& keys);
    std::string name_table_;
    std::vector<std::string> keys_;
};

namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<select_t> final {
                msgpack::object const& operator()(msgpack::object const& o, select_t& v) const {
                    if (o.type != msgpack::type::ARRAY) {
                        throw msgpack::type_error();
                    }

                    if (o.via.array.size != 2) {
                        throw msgpack::type_error();
                    }

                    v = select_t(
                        o.via.array.ptr[0].as<std::string>(),
                        o.via.array.ptr[1].as<std::vector<std::string>>());
                    return o;
                }
            };

            template<>
            struct pack<select_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, select_t const& v) const {
                    o.pack_array(3);
                    o.pack_str_body(v.name_table_.data(), v.name_table_.size());
                    o.pack_array(v.keys_.size());
                    for(const auto&i:v.keys_){
                        msgpack::pack(o,i);
                    }
                    return o;
                }
            };

            template<>
            struct object_with_zone<select_t> final {
                void operator()(msgpack::object::with_zone& o, select_t const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 2;
                    o.via.array.ptr = static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size, MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v.name_table_, o.zone);
                    o.via.array.ptr[1] = msgpack::object(v.keys_, o.zone);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack