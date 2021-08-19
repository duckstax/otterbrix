#pragma once
#include "base.hpp"
#include "select.hpp"
#include "insert.hpp"
#include <msgpack.hpp>
#include <variant>

class protocol_t {
public:
    protocol_t() = default;
    template<class T>
    protocol_t(const std::string& uid, protocol_op opType, T&& data)
        : uid_(uid)
        , op_type(opType)
        , data_(data) {}


    std::string uid_;
    protocol_op op_type;
    std::variant<select_t, insert_t, erase_t>  data_;
};

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<protocol_t> {
                msgpack::object const& operator()(msgpack::object const& o, protocol_t& v) const {
                    if (o.type != msgpack::type::ARRAY) {
                        throw msgpack::type_error();
                    }

                    if (o.via.array.size != 3) {
                        throw msgpack::type_error();
                    }

                    uint32_t op = o.via.array.ptr[1].as<uint32_t>();
                    std::variant<select_t, insert_t, erase_t> data;
                    switch (static_cast<protocol_op>(op)) {
                        case protocol_op::create_collection:
                            break;
                        case protocol_op::create_database:
                            break;
                        case protocol_op::select: {
                            data.emplace<select_t>(o.via.array.ptr[2].as<select_t>() );
                        }

                        case protocol_op::insert: {
                            data.emplace<insert_t>(o.via.array.ptr[2].as<insert_t>() );
                        }
                        case protocol_op::erase: {
                        }
                    }

                    v = protocol_t(
                        o.via.array.ptr[0].as<std::string>(),
                        static_cast<protocol_op>(op),
                        std::move(data));
                    return o;
                }
            };

            template<>
            struct pack<protocol_t> {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, protocol_t const& v) const {
                    o.pack_array(3);
                    o.pack_str_body(v.uid_.data(),v.uid_.size());
                    o.pack_uint32(v.op_type);

                    switch (static_cast<protocol_op>(v.op_type)) {
                        case protocol_op::create_collection:
                            break;
                        case protocol_op::create_database:
                            break;
                        case protocol_op::select: {
                            auto data = std::get<select_t>(v.data_);
                            msgpack::pack(o,data);
                        }

                        case protocol_op::insert: {
                            auto data = std::get<insert_t>(v.data_);
                            msgpack::pack(o,data);
                        }
                        case protocol_op::erase: {
                        }
                    }

                    return o;
                }
            };

            template<>
            struct object_with_zone<protocol_t> {
                void operator()(msgpack::object::with_zone& o, protocol_t const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 3;
                    o.via.array.ptr = static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size, MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v.uid_, o.zone);
                    o.via.array.ptr[1] = msgpack::object(static_cast<uint32_t>(v.op_type), o.zone);

                    switch (static_cast<protocol_op>(v.op_type)) {
                        case protocol_op::create_collection:
                            break;
                        case protocol_op::create_database:
                            break;
                        case protocol_op::select: {
                            o.via.array.ptr[2] = msgpack::object(std::get<select_t>(v.data_), o.zone);
                        }

                        case protocol_op::insert: {
                            o.via.array.ptr[2] = msgpack::object(std::get<insert_t>(v.data_), o.zone);
                        }
                        case protocol_op::erase: {
                        }
                    }

                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack