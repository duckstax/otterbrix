#include "protocol.hpp"
#include <msgpack.hpp>

struct protocol_t {
    protocol_t() = default;
    protocol_t(uint64_t uuid1, uint64_t uuid2, uint32_t op, const std::string& body)
    : uuid1(uuid1)
    , uuid2(uuid2)
    , op(op)
    , body(std::move(body)) {}
    uint64_t uuid1;
    uint64_t uuid2;
    uint32_t op;
    std::string body;
    MSGPACK_DEFINE(uuid1, uuid2, op, body);
};

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<select_t> {
                msgpack::object const& operator()(msgpack::object const& o, select_t& v) const {
                    if (o.type != msgpack::type::ARRAY) throw msgpack::type_error();
                    if (o.via.array.size != 2) throw msgpack::type_error();
                    v = my_class(
                        o.via.array.ptr[0].as<std::string>(),
                        o.via.array.ptr[1].as<int>());
                    return o;
                }
            };

            template<>
            struct pack<select_t> {
                template <typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, select_t const& v) const {
                    // packing member variables as an array.
                    o.pack_array(2);
                    o.pack(v.get_name());
                    o.pack(v.get_age());
                    return o;
                }
            };

            template <>
            struct object_with_zone<select_t> {
                void operator()(msgpack::object::with_zone& o, select_t const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 2;
                    o.via.array.ptr = static_cast<msgpack::object*>(
                        o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size, MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v.get_name(), o.zone);
                    o.via.array.ptr[1] = msgpack::object(v.get_age(), o.zone);
                }
            };

        } // namespace adaptor
    } // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack