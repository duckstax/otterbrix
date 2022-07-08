#pragma once

#include <boost/beast/core/span.hpp>
#include <components/document/msgpack/msgpack_encoder.hpp>
#include <components/serialize/serialize.hpp>
#include <msgpack/adaptor/list.hpp>
#include "components/ql/ql_statement.hpp"

struct update_many_t : ql_statement_t {
    update_many_t(const database_name_t &database, const collection_name_t &collection, components::document::document_ptr condition,
                 components::document::document_ptr update, bool upsert);
    update_many_t() = default;
    update_many_t(const update_many_t&) = default;
    update_many_t& operator=(const update_many_t&) = default;
    update_many_t(update_many_t&&) = default;
    update_many_t& operator=(update_many_t&&) = default;
    ~update_many_t();

    components::document::document_ptr condition_;
    components::document::document_ptr update_;
    bool upsert_ {false};
};

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<update_many_t> final {
                msgpack::object const &operator()(msgpack::object const &o, update_many_t &v) const {
                    if (o.type != msgpack::type::ARRAY) {
                        throw msgpack::type_error();
                    }

                    if (o.via.array.size != 5) {
                        throw msgpack::type_error();
                    }

                    auto database = o.via.array.ptr[0].as<std::string>();
                    auto collection = o.via.array.ptr[1].as<std::string>();
                    auto condition = o.via.array.ptr[2].as<components::document::document_ptr>();
                    auto update = o.via.array.ptr[3].as<components::document::document_ptr>();
                    auto upsert = o.via.array.ptr[4].as<bool>();
                    v = update_many_t(database, collection, condition, update, upsert);
                    return o;
                }
            };

            template<>
            struct pack<update_many_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream> &o, update_many_t const &v) const {
                    o.pack_array(5);
                    o.pack(v.database_);
                    o.pack(v.collection_);
                    o.pack(v.condition_);
                    o.pack(v.update_);
                    o.pack(v.upsert_);
                    return o;
                }
            };

            template<>
            struct object_with_zone<update_many_t> final {
                void operator()(msgpack::object::with_zone &o, update_many_t const &v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 5;
                    o.via.array.ptr = static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size, MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v.database_, o.zone);
                    o.via.array.ptr[1] = msgpack::object(v.collection_, o.zone);
                    o.via.array.ptr[2] = msgpack::object(v.condition_, o.zone);
                    o.via.array.ptr[3] = msgpack::object(v.update_, o.zone);
                    o.via.array.ptr[4] = msgpack::object(v.upsert_, o.zone);
                }
            };

        } // namespace adaptor
    } // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
