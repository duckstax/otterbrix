#pragma once
#include <msgpack.hpp>

struct response_select_t final {
    response_select_t() = default;
    std::optional<std::list<msgpack::object>> keys_;
};


/*
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<response_select_t> final {
                msgpack::object const& operator()(msgpack::object const& o, response_select_t& v) const {
                    if (o.type != msgpack::type::ARRAY) {
                        throw msgpack::type_error();
                    }

                    if (o.via.array.size != 2) {
                        throw msgpack::type_error();
                    }

                    v = response_select_t(
                        o.via.array.ptr[0].as<std::string>(),
                        o.via.array.ptr[1].as<std::vector<std::string>>());
                    return o;
                }
            };

            template<>
            struct pack<response_select_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, response_select_t const& v) const {
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
            struct object_with_zone<response_select_t> final {
                void operator()(msgpack::object::with_zone& o, response_select_t const& v) const {
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
 */