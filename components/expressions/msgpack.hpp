#pragma once

#include "compare_expression.hpp"
#include "expression.hpp"
#include <msgpack.hpp>

namespace components::expressions {

    inline components::expressions::compare_expression_ptr to_compare_expression(const msgpack::object& msg_object,
                                                                                 std::pmr::memory_resource* resource) {
        if (msg_object.type != msgpack::type::ARRAY) {
            throw msgpack::type_error();
        }
        if (msg_object.via.array.size != 5) {
            throw msgpack::type_error();
        }
        auto type = static_cast<components::expressions::compare_type>(msg_object.via.array.ptr[0].as<uint8_t>());
        auto key_left = msg_object.via.array.ptr[1].as<components::expressions::key_t>();
        auto key_right = msg_object.via.array.ptr[2].as<components::expressions::key_t>();
        auto value = msg_object.via.array.ptr[3].as<core::parameter_id_t>();
        compare_expression_ptr result;
        if (key_right.is_null()) {
            result = components::expressions::make_compare_expression(resource, type, key_left, value);
        } else {
            result = components::expressions::make_compare_expression(resource, type, key_left, key_right);
        }
        for (uint32_t i = 0; i < msg_object.via.array.ptr[4].via.array.size; ++i) {
            result->append_child(to_compare_expression(msg_object.via.array.ptr[4].via.array.ptr[i], resource));
        }
        return result;
    }
    inline components::expressions::expression_ptr to_expression(const msgpack::object& msg_object,
                                                                 std::pmr::memory_resource* resource) {
        if (msg_object.type != msgpack::type::ARRAY) {
            throw msgpack::type_error();
        }
        if (msg_object.via.array.size != 2) {
            throw msgpack::type_error();
        }
        auto group = static_cast<components::expressions::expression_group>(msg_object.via.array.ptr[0].as<uint8_t>());
        switch (group) {
            case components::expressions::expression_group::compare:
                return to_compare_expression(msg_object.via.array.ptr[1], resource);
            default:
                //todo: error not valid expression
                break;
        }
        return nullptr;
    }

    inline std::pmr::vector<expression_ptr> to_expressions(const msgpack::object& msg_object,
                                                           std::pmr::memory_resource* resource) {
        std::pmr::vector<expression_ptr> result(resource);
        if (msg_object.type != msgpack::type::ARRAY) {
            throw msgpack::type_error();
        }
        for (const auto& expr : msg_object.via.array) {
            result.emplace_back(to_expression(expr, resource));
        }
        return result;
    }
} // namespace components::expressions

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
                    auto s = o.as<std::string>();
                    if (s.empty()) {
                        v = components::expressions::key_t{};
                    } else {
                        v = components::expressions::key_t{s};
                    }
                    return o;
                }
            };

            template<>
            struct pack<components::expressions::key_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, components::expressions::key_t const& v) const {
                    if (v.is_null()) {
                        o.pack(std::string{});
                    } else {
                        o.pack(static_cast<std::string>(v.as_string()));
                    }
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::expressions::key_t> final {
                void operator()(msgpack::object::with_zone& o, components::expressions::key_t const& v) const {
                    if (v.is_null()) {
                        msgpack::object(std::string{}, o.zone);
                    } else {
                        msgpack::object(v.as_string(), o.zone);
                    }
                }
            };

            template<>
            struct pack<components::expressions::compare_expression_ptr> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o,
                                           components::expressions::compare_expression_ptr const& v) const {
                    o.pack_array(5);
                    o.pack(static_cast<uint8_t>(v->type()));
                    o.pack(v->key_left());
                    o.pack(v->key_right());
                    o.pack(v->value());
                    o.pack_array(static_cast<uint32_t>(v->children().size()));
                    for (const auto& child : v->children()) {
                        o.pack(child);
                    }
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::expressions::compare_expression_ptr> final {
                void operator()(msgpack::object::with_zone& o,
                                components::expressions::compare_expression_ptr const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 5;
                    o.via.array.ptr =
                        static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size,
                                                                            MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(static_cast<uint8_t>(v->type()), o.zone);
                    o.via.array.ptr[1] = msgpack::object(v->key_left(), o.zone);
                    o.via.array.ptr[2] = msgpack::object(v->key_right(), o.zone);
                    o.via.array.ptr[3] = msgpack::object(v->value(), o.zone);
                    o.via.array.ptr[4].type = type::ARRAY;
                    o.via.array.ptr[4].via.array.size = static_cast<uint32_t>(v->children().size());
                    for (uint32_t i = 0; i < v->children().size(); ++i) {
                        o.via.array.ptr[4].via.array.ptr[i] = msgpack::object(v->children().at(i), o.zone);
                    }
                }
            };

            template<>
            struct pack<components::expressions::expression_ptr> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o,
                                           components::expressions::expression_ptr const& v) const {
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
                    o.via.array.ptr =
                        static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size,
                                                                            MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(static_cast<uint8_t>(v->group()), o.zone);
                    switch (v->group()) {
                        case components::expressions::expression_group::compare:
                            o.via.array.ptr[1] = msgpack::object(
                                reinterpret_cast<components::expressions::compare_expression_ptr const&>(v),
                                o.zone);
                            break;
                        default:
                            //todo: error not valid expression
                            break;
                    }
                }
            };

            template<>
            struct pack<std::pmr::vector<components::expressions::expression_ptr>> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o,
                                           std::pmr::vector<components::expressions::expression_ptr> const& v) const {
                    o.pack_array(v.size());
                    for (const auto& expr : v) {
                        o.pack(expr);
                    }
                    return o;
                }
            };

            template<>
            struct object_with_zone<std::pmr::vector<components::expressions::expression_ptr>> final {
                void operator()(msgpack::object::with_zone& o,
                                std::pmr::vector<components::expressions::expression_ptr> const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = v.size();
                    o.via.array.ptr =
                        static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size,
                                                                            MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    for (size_t i = 0; i < v.size(); i++) {
                        o.via.array.ptr[i] = msgpack::object(v.at(i), o.zone);
                    }
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
