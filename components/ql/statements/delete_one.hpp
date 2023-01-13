#pragma once

#include <boost/beast/core/span.hpp>
#include <components/document/msgpack/msgpack_encoder.hpp>
#include <components/ql/aggregate.hpp>
#include <msgpack.hpp>
#include <msgpack/zone.hpp>
#include <msgpack/adaptor/list.hpp>

namespace components::ql {

    struct delete_one_t final : ql_statement_t {
        delete_one_t(const database_name_t& database, const collection_name_t& collection, components::ql::aggregate::match_t match);
        explicit delete_one_t(components::ql::aggregate_statement_raw_ptr condition);
        delete_one_t() = default;
        delete_one_t(const delete_one_t&) = default;
        delete_one_t& operator=(const delete_one_t&) = default;
        delete_one_t(delete_one_t&&) = default;
        delete_one_t& operator=(delete_one_t&&) = default;
        ~delete_one_t() final;
        components::ql::aggregate::match_t match_;
    };
} // namespace components::ql

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<components::ql::delete_one_t> final {
                msgpack::object const& operator()(msgpack::object const& o, components::ql::delete_one_t& v) const {
                    if (o.type != msgpack::type::ARRAY) {
                        throw msgpack::type_error();
                    }

                    if (o.via.array.size != 3) {
                        throw msgpack::type_error();
                    }

                    auto database = o.via.array.ptr[0].as<std::string>();
                    auto collection = o.via.array.ptr[1].as<std::string>();
                    auto condition = o.via.array.ptr[2].as<components::ql::aggregate::match_t>();
                    v = components::ql::delete_one_t(database, collection, condition);
                    return o;
                }
            };

            template<>
            struct pack<components::ql::delete_one_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, components::ql::delete_one_t const& v) const {
                    o.pack_array(3);
                    o.pack(v.database_);
                    o.pack(v.collection_);
                    o.pack(v.match_);
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::ql::delete_one_t> final {
                void operator()(msgpack::object::with_zone& o, components::ql::delete_one_t const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 3;
                    o.via.array.ptr = static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size, MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v.database_, o.zone);
                    o.via.array.ptr[1] = msgpack::object(v.collection_, o.zone);
                    o.via.array.ptr[2] = msgpack::object(v.match_, o.zone);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
