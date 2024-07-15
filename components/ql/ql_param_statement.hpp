#pragma once

#include "ql_statement.hpp"
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/document/msgpack/msgpack_encoder.hpp>
#include <components/document/value.hpp>
#include <components/expressions/msgpack.hpp>
#include <core/pmr.hpp>
#include <msgpack.hpp>
#include <msgpack/adaptor/list.hpp>
#include <msgpack/zone.hpp>

namespace components::ql {

    using expr_value_t = document::value_t;

    class tape_wrapper : public boost::intrusive_ref_counter<tape_wrapper> {
    public:
        explicit tape_wrapper(std::pmr::memory_resource* resource)
            : tape_(new document::impl::base_document(resource)) {}
        ~tape_wrapper() { delete tape_; }
        document::impl::base_document* tape() const { return tape_; }

    private:
        document::impl::base_document* tape_;
    };

    struct storage_parameters {
        std::pmr::unordered_map<core::parameter_id_t, expr_value_t> parameters;

        explicit storage_parameters(std::pmr::memory_resource* resource)
            : parameters(resource)
            , tape_(new tape_wrapper(resource)) {}

        document::impl::base_document* tape() const { return tape_->tape(); }
        std::pmr::memory_resource* resource() const { return parameters.get_allocator().resource(); }

    private:
        boost::intrusive_ptr<tape_wrapper> tape_; // TODO: make into unique_ptr
    };

    template<class Value>
    void add_parameter(storage_parameters& storage, core::parameter_id_t id, Value value) {
        storage.parameters.emplace(id, expr_value_t(storage.tape(), value));
    }

    inline void add_parameter(storage_parameters& storage, core::parameter_id_t id, expr_value_t value) {
        storage.parameters.emplace(id, value);
    }

    const expr_value_t& get_parameter(const storage_parameters* storage, core::parameter_id_t id);

    class ql_param_statement_t : public ql_statement_t {
    public:
        ql_param_statement_t(statement_type type,
                             database_name_t database,
                             collection_name_t collection,
                             std::pmr::memory_resource* resource);
        ql_param_statement_t(std::pmr::memory_resource* resource)
            : values_(resource) {}

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

    inline components::document::value_t to_structure_(const msgpack::object& msg_object,
                                                       components::document::impl::base_document* tape) {
        switch (msg_object.type) {
            case msgpack::type::NIL:
                return components::document::value_t(tape, nullptr);
            case msgpack::type::BOOLEAN:
                return components::document::value_t(tape, msg_object.via.boolean);
            case msgpack::type::POSITIVE_INTEGER:
                return components::document::value_t(tape, msg_object.via.u64);
            case msgpack::type::NEGATIVE_INTEGER:
                return components::document::value_t(tape, msg_object.via.i64);
            case msgpack::type::FLOAT32:
            case msgpack::type::FLOAT64:
                return components::document::value_t(tape, msg_object.via.f64);
            case msgpack::type::STR:
                return components::document::value_t(tape,
                                                     std::string_view(msg_object.via.str.ptr, msg_object.via.str.size));
            default:
                return components::document::value_t();
        }
    }

    inline components::ql::storage_parameters to_storage_parameters(const msgpack::object& msg_object,
                                                                    std::pmr::memory_resource* resource) {
        components::ql::storage_parameters result(resource);
        if (msg_object.type != msgpack::type::MAP) {
            throw msgpack::type_error();
        }
        for (uint32_t i = 0; i < msg_object.via.map.size; ++i) {
            auto key = msg_object.via.map.ptr[i].key.as<core::parameter_id_t>();
            auto value = to_structure_(msg_object.via.map.ptr[i].val, result.tape());
            result.parameters.emplace(key, value);
        }
        return result;
    }
} // namespace components::ql

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct pack<components::ql::storage_parameters> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o,
                                           components::ql::storage_parameters const& v) const {
                    o.pack_map(v.parameters.size());
                    for (auto it : v.parameters) {
                        o.pack(it.first);
                        to_msgpack_(o, it.second.get_element());
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
                        to_msgpack_(it.second.get_element(), o.via.map.ptr[i].val);
                        ++i;
                    }
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
