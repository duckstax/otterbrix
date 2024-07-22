#pragma once

#include "components/ql/ql_statement.hpp"
#include <boost/beast/core/span.hpp>
#include <components/document/msgpack/msgpack_encoder.hpp>
#include <msgpack.hpp>
#include <msgpack/adaptor/list.hpp>
#include <msgpack/zone.hpp>

namespace components::ql {
    struct insert_one_t : ql_statement_t {
        explicit insert_one_t(const database_name_t& database,
                              const collection_name_t& collection,
                              components::document::document_ptr document);
        explicit insert_one_t(std::pmr::memory_resource* resource)
            : document_(document::make_document(resource)) {}
        insert_one_t(const insert_one_t&) = default;
        insert_one_t& operator=(const insert_one_t&) = default;
        insert_one_t(insert_one_t&&) = default;
        insert_one_t& operator=(insert_one_t&&) = default;
        ~insert_one_t();
        components::document::document_ptr document_;
    };

    inline insert_one_t to_insert_one(const msgpack::object& msg_object, std::pmr::memory_resource* resource) {
        if (msg_object.type != msgpack::type::ARRAY) {
            throw msgpack::type_error();
        }
        if (msg_object.via.array.size != 3) {
            throw msgpack::type_error();
        }
        auto database = msg_object.via.array.ptr[0].as<std::string>();
        auto collection = msg_object.via.array.ptr[1].as<std::string>();
        auto document = document::to_document(msg_object.via.array.ptr[2], resource);
        return insert_one_t(database, collection, document);
    }
} // namespace components::ql

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct pack<components::ql::insert_one_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, components::ql::insert_one_t const& v) const {
                    o.pack_array(3);
                    o.pack(v.database_);
                    o.pack(v.collection_);
                    o.pack(v.document_);
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::ql::insert_one_t> final {
                void operator()(msgpack::object::with_zone& o, components::ql::insert_one_t const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 3;
                    o.via.array.ptr =
                        static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size,
                                                                            MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v.database_, o.zone);
                    o.via.array.ptr[1] = msgpack::object(v.collection_, o.zone);
                    o.via.array.ptr[2] = msgpack::object(v.document_, o.zone);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
