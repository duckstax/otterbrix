#pragma once

#include <components/document/msgpack/msgpack_encoder.hpp>
#include <components/expressions/msgpack.hpp>
#include "ql_statement.hpp"
#include <msgpack.hpp>
#include <msgpack/zone.hpp>
#include <msgpack/adaptor/list.hpp>

namespace components::ql {

    using expr_value_t = ::document::wrapper_value_t;
    using storage_parameters = std::unordered_map<core::parameter_id_t, expr_value_t>;

    template<class Value>
    void add_parameter(storage_parameters &storage, core::parameter_id_t id, Value value) {
        storage.emplace(id, expr_value_t(::document::impl::new_value(value).detach()));
    }

    const expr_value_t& get_parameter(const storage_parameters *storage, core::parameter_id_t id);


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
        storage_parameters values_;
    };

} // namespace components::ql


// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<components::ql::storage_parameters> final {
                msgpack::object const& operator()(msgpack::object const& o, components::ql::storage_parameters& v) const {
                    if (o.type != msgpack::type::MAP) {
                        throw msgpack::type_error();
                    }
                    for (uint32_t i = 0; i < o.via.map.size; ++i) {
                        auto key = o.via.map.ptr[i].key.as<core::parameter_id_t>();
                        auto value = to_structure_(o.via.map.ptr[i].val);
                        v.emplace(key, document::wrapper_value_t{value});
                    }
                    return o;
                }
            };

            template<>
            struct pack<components::ql::storage_parameters> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, components::ql::storage_parameters const& v) const {
                    o.pack_map(v.size());
                    for (auto it : v) {
                        o.pack(it.first);
                        to_msgpack_(o, *it.second);
                    }
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::ql::storage_parameters> final {
                void operator()(msgpack::object::with_zone& o, components::ql::storage_parameters const& v) const {
                    o.type = type::MAP;
                    o.via.array.size = static_cast<uint32_t>(v.size());
                    o.via.array.ptr = static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size, MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    uint32_t i = 0;
                    for (auto it : v) {
                        o.via.map.ptr[i].key = msgpack::object(it.first, o.zone);
                        to_msgpack_(*it.second, o.via.map.ptr[i].val);
                        ++i;
                    }
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
