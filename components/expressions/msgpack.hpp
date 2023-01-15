#pragma once

#include <msgpack.hpp>
#include <actor-zeta/detail/pmr/default_resource.hpp>
#include "expression.hpp"
#include "compare_expression.hpp"

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            // parameter_id_t
            template<>
            struct convert<core::parameter_id_t> final {
                msgpack::object const& operator()(msgpack::object const& o, core::parameter_id_t& v) const {
                    v = core::parameter_id_t{o.as<uint16_t>()};
                    return o;
                }
            };

            template<>
            struct pack<core::parameter_id_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, core::parameter_id_t const& v) const {
                    o.pack(v.t);
                    return o;
                }
            };

            template<>
            struct object_with_zone<core::parameter_id_t> final {
                void operator()(msgpack::object::with_zone& o, core::parameter_id_t const& v) const {
                    msgpack::object(v.t, o.zone);
                }
            };



            // key_t
            //todo: other types key
            template<>
            struct convert<components::expressions::key_t> final {
                msgpack::object const& operator()(msgpack::object const& o, components::expressions::key_t& v) const {
                    v = components::expressions::key_t(o.as<std::string>());
                    return o;
                }
            };

            template<>
            struct pack<components::expressions::key_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, components::expressions::key_t const& v) const {
                    o.pack(static_cast<std::string>(v.as_string()));
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::expressions::key_t> final {
                void operator()(msgpack::object::with_zone& o, components::expressions::key_t const& v) const {
                    msgpack::object(v.as_string(), o.zone);
                }
            };



            // compare_expression_t
            template<>
            struct convert<components::expressions::compare_expression_ptr> final {
                msgpack::object const& operator()(msgpack::object const& o, components::expressions::compare_expression_ptr& v) const {
                    if (o.type != msgpack::type::ARRAY) {
                        throw msgpack::type_error();
                    }
                    if (o.via.array.size != 4) {
                        throw msgpack::type_error();
                    }
                    auto type = static_cast<components::expressions::compare_type>(o.via.array.ptr[0].as<uint8_t>());
                    auto key = o.via.array.ptr[1].as<components::expressions::key_t>();
                    auto value = o.via.array.ptr[2].as<core::parameter_id_t>();
                    v = components::expressions::make_compare_expression(actor_zeta::detail::pmr::get_default_resource(), type, key, value);
                    for (uint32_t i = 0; i < o.via.array.ptr[3].via.array.size; ++i) {
                        v->append_child(o.via.array.ptr[3].via.array.ptr[i].as<components::expressions::compare_expression_ptr>());
                    }
                    return o;
                }
            };

            template<>
            struct pack<components::expressions::compare_expression_ptr> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, components::expressions::compare_expression_ptr const& v) const {
                    o.pack_array(4);
                    o.pack(static_cast<uint8_t>(v->type()));
                    o.pack(v->key());
                    o.pack(v->value());
                    o.pack_array(static_cast<uint32_t>(v->children().size()));
                    for (const auto &child : v->children()) {
                        o.pack(child);
                    }
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::expressions::compare_expression_ptr> final {
                void operator()(msgpack::object::with_zone& o, components::expressions::compare_expression_ptr const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 4;
                    o.via.array.ptr = static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size, MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(static_cast<uint8_t>(v->type()), o.zone);
                    o.via.array.ptr[1] = msgpack::object(v->key(), o.zone);
                    o.via.array.ptr[2] = msgpack::object(v->value(), o.zone);
                    o.via.array.ptr[3].type = type::ARRAY;
                    o.via.array.ptr[3].via.array.size = static_cast<uint32_t>(v->children().size());
                    for (uint32_t i = 0; i < v->children().size(); ++i) {
                        o.via.array.ptr[3].via.array.ptr[i] = msgpack::object(v->children().at(i), o.zone);
                    }
                }
            };



            // expression_ptr
            template<>
            struct convert<components::expressions::expression_ptr> final {
                msgpack::object const& operator()(msgpack::object const& o, components::expressions::expression_ptr& v) const {
                    if (o.type != msgpack::type::ARRAY) {
                        throw msgpack::type_error();
                    }
                    if (o.via.array.size != 2) {
                        throw msgpack::type_error();
                    }
                    auto group = static_cast<components::expressions::expression_group>(o.via.array.ptr[0].as<uint8_t>());
                    switch (group) {
                    case components::expressions::expression_group::compare:
                        v = o.via.array.ptr[1].as<components::expressions::compare_expression_ptr>();
                        break;
                    default:
                        //todo: error not valid expression
                        break;
                    }
                    return o;
                }
            };

            template<>
            struct pack<components::expressions::expression_ptr> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, components::expressions::expression_ptr const& v) const {
                    o.pack_array(2);
                    o.pack(static_cast<uint8_t>(v->group()));
                    switch (v->group()) {
                    case components::expressions::expression_group::compare:
                        o.pack(reinterpret_cast<components::expressions::compare_expression_ptr const&>(v));
                        break;
                    default:
                        //todo: error not valid expression
                        break;
                    }
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::expressions::expression_ptr> final {
                void operator()(msgpack::object::with_zone& o, components::expressions::expression_ptr const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 2;
                    o.via.array.ptr = static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size, MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(static_cast<uint8_t>(v->group()), o.zone);
                    switch (v->group()) {
                    case components::expressions::expression_group::compare:
                        o.via.array.ptr[1] = msgpack::object(reinterpret_cast<components::expressions::compare_expression_ptr const&>(v), o.zone);
                        break;
                    default:
                        //todo: error not valid expression
                        break;
                    }
                }
            };

        } // namespace adaptor
    } // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
