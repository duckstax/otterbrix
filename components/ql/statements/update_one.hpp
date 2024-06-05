#pragma once

#include <boost/beast/core/span.hpp>
#include <components/document/msgpack/msgpack_encoder.hpp>
#include <components/ql/aggregate.hpp>
#include <msgpack.hpp>
#include <msgpack/adaptor/list.hpp>
#include <msgpack/zone.hpp>

namespace components::ql {

    struct update_one_t final : ql_param_statement_t {
        update_one_t(const database_name_t& database,
                     const collection_name_t& collection,
                     const components::ql::aggregate::match_t& match,
                     const storage_parameters& parameters,
                     const components::document::document_ptr& update,
                     bool upsert);
        explicit update_one_t(components::ql::aggregate_statement_raw_ptr condition,
                              const components::document::document_ptr& update,
                              bool upsert);
        update_one_t() = default;
        ~update_one_t() final;
        components::ql::aggregate::match_t match_;
        components::document::document_ptr update_;
        bool upsert_{false};
    };
} // namespace components::ql

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<components::ql::update_one_t> final {
                msgpack::object const& operator()(msgpack::object const& o, components::ql::update_one_t& v) const {
                    if (o.type != msgpack::type::ARRAY) {
                        throw msgpack::type_error();
                    }
                    if (o.via.array.size != 6) {
                        throw msgpack::type_error();
                    }
                    auto database = o.via.array.ptr[0].as<std::string>();
                    auto collection = o.via.array.ptr[1].as<std::string>();
                    auto condition = o.via.array.ptr[2].as<components::ql::aggregate::match_t>();
                    auto parameters = o.via.array.ptr[3].as<components::ql::storage_parameters>();
                    auto update = o.via.array.ptr[4].as<components::document::document_ptr>();
                    auto upsert = o.via.array.ptr[5].as<bool>();
                    v = components::ql::update_one_t(database, collection, condition, parameters, update, upsert);
                    return o;
                }
            };

            template<>
            struct pack<components::ql::update_one_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, components::ql::update_one_t const& v) const {
                    o.pack_array(6);
                    o.pack(v.database_);
                    o.pack(v.collection_);
                    o.pack(v.match_);
                    o.pack(v.parameters());
                    o.pack(v.update_);
                    o.pack(v.upsert_);
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::ql::update_one_t> final {
                void operator()(msgpack::object::with_zone& o, components::ql::update_one_t const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 4;
                    o.via.array.ptr =
                        static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size,
                                                                            MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v.database_, o.zone);
                    o.via.array.ptr[1] = msgpack::object(v.collection_, o.zone);
                    o.via.array.ptr[2] = msgpack::object(v.match_, o.zone);
                    o.via.array.ptr[3] = msgpack::object(v.parameters(), o.zone);
                    o.via.array.ptr[4] = msgpack::object(v.update_, o.zone);
                    o.via.array.ptr[5] = msgpack::object(v.upsert_, o.zone);
                }
            };

        } // namespace adaptor
    } // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
