#pragma once

#include "ql_statement.hpp"
#include <components/document/msgpack/msgpack_encoder.hpp>
#include <components/document/value.hpp>
#include <components/expressions/msgpack.hpp>
#include <core/pmr.hpp>
#include <msgpack.hpp>
#include <msgpack/adaptor/list.hpp>
#include <msgpack/zone.hpp>

namespace components::ql {

    using expr_value_t = document::value_t;

    struct storage_parameters {
        std::unordered_map<core::parameter_id_t, expr_value_t> parameters;
        document::impl::mutable_document* tape; // TODO: remove memory leak
    };

    template<class Value>
    void add_parameter(storage_parameters& storage,
                       core::parameter_id_t id,
                       Value value,
                       std::pmr::memory_resource* resource) {
        storage.parameters.emplace(id, expr_value_t(resource, storage.tape, value));
    }

    inline void add_parameter(storage_parameters& storage, core::parameter_id_t id, expr_value_t value) {
        storage.parameters.emplace(id, value);
    }

    const expr_value_t& get_parameter(const storage_parameters* storage, core::parameter_id_t id);

    class ql_param_statement_t : public ql_statement_t {
    public:
        ql_param_statement_t(statement_type type, database_name_t database, collection_name_t collection);
        ql_param_statement_t() = default;

        bool is_parameters() const override;
        auto parameters() const -> const storage_parameters&;
        auto take_parameters() -> storage_parameters;
        auto set_parameters(const storage_parameters& parameters) -> void;

        auto next_id() -> core::parameter_id_t;

        template<class Value>
        void add_parameter(core::parameter_id_t id, Value value) {
            components::ql::add_parameter(values_, id, value);
        }

        template<class Value>
        core::parameter_id_t add_parameter(Value value) {
            auto id = next_id();
            add_parameter(id, value);
            return id;
        }

        auto parameter(core::parameter_id_t id) const -> const expr_value_t&;

    private:
        uint16_t counter_{0};
        storage_parameters values_{};
    };

} // namespace components::ql

inline const components::document::value_t to_structure_(const msgpack::object& msg_object,
                                                         components::document::impl::mutable_document* tape,
                                                         std::pmr::memory_resource* resource) {
    switch (msg_object.type) {
        case msgpack::type::NIL:
            return components::document::value_t(resource, tape, nullptr);
        case msgpack::type::BOOLEAN:
            return components::document::value_t(resource, tape, msg_object.via.boolean);
        case msgpack::type::POSITIVE_INTEGER:
            return components::document::value_t(resource, tape, msg_object.via.u64);
        case msgpack::type::NEGATIVE_INTEGER:
            return components::document::value_t(resource, tape, msg_object.via.i64);
        case msgpack::type::FLOAT32:
        case msgpack::type::FLOAT64:
            return components::document::value_t(resource, tape, msg_object.via.f64);
        case msgpack::type::STR:
            return components::document::value_t(resource,
                                                 tape,
                                                 std::string_view(msg_object.via.str.ptr, msg_object.via.str.size));
        default:
            return components::document::value_t();
    }
}

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<components::ql::storage_parameters> final {
                msgpack::object const& operator()(msgpack::object const& o,
                                                  components::ql::storage_parameters& v) const {
                    if (o.type != msgpack::type::MAP) {
                        throw msgpack::type_error();
                    }
                    v.tape = new components::document::impl::mutable_document(core::pmr::default_resource());
                    for (uint32_t i = 0; i < o.via.map.size; ++i) {
                        auto key = o.via.map.ptr[i].key.as<core::parameter_id_t>();
                        auto value = to_structure_(o.via.map.ptr[i].val, v.tape, core::pmr::default_resource());
                        v.parameters.emplace(key, value);
                    }
                    return o;
                }
            };

            template<>
            struct pack<components::ql::storage_parameters> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o,
                                           components::ql::storage_parameters const& v) const {
                    o.pack_map(v.parameters.size());
                    for (auto it : v.parameters) {
                        o.pack(it.first);
                        if (it.second.is_immut()) {
                            to_msgpack_(o, it.second.get_immut());
                        } else {
                            to_msgpack_(o, it.second.get_mut());
                        }
                    }
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::ql::storage_parameters> final {
                void operator()(msgpack::object::with_zone& o, components::ql::storage_parameters const& v) const {
                    o.type = type::MAP;
                    o.via.array.size = static_cast<uint32_t>(v.parameters.size());
                    o.via.array.ptr =
                        static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size,
                                                                            MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    uint32_t i = 0;
                    for (auto it : v.parameters) {
                        o.via.map.ptr[i].key = msgpack::object(it.first, o.zone);
                        if (it.second.is_immut()) {
                            to_msgpack_(it.second.get_immut(), o.via.map.ptr[i].val);
                        } else {
                            to_msgpack_(it.second.get_mut(), o.via.map.ptr[i].val);
                        }
                        ++i;
                    }
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
