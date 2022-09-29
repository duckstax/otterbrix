#pragma once

#include <boost/beast/core/span.hpp>
#include "base.hpp"
#include "components/document/msgpack/msgpack_encoder.hpp"
#include <msgpack/adaptor/list.hpp>

struct insert_many_t : statement_t {
    insert_many_t(const database_name_t & database, const collection_name_t & collection, std::list<components::document::document_ptr> documents);
    insert_many_t() = default;
    insert_many_t(const insert_many_t&) = default;
    insert_many_t& operator=(const insert_many_t&) = default;
    insert_many_t(insert_many_t&&) = default;
    insert_many_t& operator=(insert_many_t&&) = default;
    ~insert_many_t();
    std::list<components::document::document_ptr> documents_;
};

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<insert_many_t> final {
                msgpack::object const& operator()(msgpack::object const& o, insert_many_t& v) const {
                    if (o.type != msgpack::type::ARRAY) {
                        throw msgpack::type_error();
                    }

                    if (o.via.array.size != 3) {
                        throw msgpack::type_error();
                    }

                    auto database = o.via.array.ptr[0].as<std::string>();
                    auto collection = o.via.array.ptr[1].as<std::string>();
                    auto documents = o.via.array.ptr[2].as<std::list<components::document::document_ptr>>();
                    v = std::move(insert_many_t(database, collection, documents));
                    return o;
                }
            };

            template<>
            struct pack<insert_many_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, insert_many_t const& v) const {
                    o.pack_array(3);
                    o.pack(v.database_);
                    o.pack(v.collection_);
                    o.pack(v.documents_);
                    return o;
                }
            };

            template<>
            struct object_with_zone<insert_many_t> final {
                void operator()(msgpack::object::with_zone& o, insert_many_t const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 3;
                    o.via.array.ptr = static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size, MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v.database_, o.zone);
                    o.via.array.ptr[1] = msgpack::object(v.collection_, o.zone);
                    o.via.array.ptr[2] = msgpack::object(v.documents_, o.zone);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
