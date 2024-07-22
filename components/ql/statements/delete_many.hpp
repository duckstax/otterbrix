#pragma once

#include <boost/beast/core/span.hpp>
#include <components/document/msgpack/msgpack_encoder.hpp>
#include <components/ql/aggregate.hpp>
#include <msgpack.hpp>
#include <msgpack/adaptor/list.hpp>
#include <msgpack/zone.hpp>

namespace components::ql {

    struct delete_many_t final : ql_param_statement_t {
        delete_many_t(const database_name_t& database,
                      const collection_name_t& collection,
                      const components::ql::aggregate::match_t& match,
                      const storage_parameters& parameters);
        delete_many_t(const database_name_t& database,
                      const collection_name_t& collection,
                      std::pmr::memory_resource* resource);
        explicit delete_many_t(components::ql::aggregate_statement_raw_ptr condition);
        delete_many_t() = default;
        ~delete_many_t() final;
        components::ql::aggregate::match_t match_;
    };

    inline delete_many_t to_delete_many(const msgpack::object& msg_object, std::pmr::memory_resource* resource) {
        if (msg_object.type != msgpack::type::ARRAY) {
            throw msgpack::type_error();
        }
        if (msg_object.via.array.size != 4) {
            throw msgpack::type_error();
        }
        auto database = msg_object.via.array.ptr[0].as<std::string>();
        auto collection = msg_object.via.array.ptr[1].as<std::string>();
        auto condition = aggregate::to_match(msg_object.via.array.ptr[2], resource);
        auto parameters = to_storage_parameters(msg_object.via.array.ptr[3], resource);
        return delete_many_t(database, collection, condition, parameters);
    }
} // namespace components::ql

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct pack<components::ql::delete_many_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, components::ql::delete_many_t const& v) const {
                    o.pack_array(4);
                    o.pack(v.database_);
                    o.pack(v.collection_);
                    o.pack(v.match_);
                    o.pack(v.parameters());
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::ql::delete_many_t> final {
                void operator()(msgpack::object::with_zone& o, components::ql::delete_many_t const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 4;
                    o.via.array.ptr =
                        static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size,
                                                                            MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v.database_, o.zone);
                    o.via.array.ptr[1] = msgpack::object(v.collection_, o.zone);
                    o.via.array.ptr[2] = msgpack::object(v.match_, o.zone);
                    o.via.array.ptr[3] = msgpack::object(v.parameters(), o.zone);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
