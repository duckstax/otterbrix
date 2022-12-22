#pragma once

#include <memory_resource>
#include <vector>

#include <msgpack.hpp>
#include <components/expressions/key.hpp>
#include "ql_statement.hpp"

namespace components::ql {

    using keys_base_storage_t = std::pmr::vector<components::expressions::key_t>;

    enum class index_type : char {
        single,
        composite,
        multikey,
        hashed,
        wildcard,
    };

    struct create_index_t
        : ql_statement_t {
        create_index_t(const database_name_t& database, const collection_name_t& collection, index_type type)
            : ql_statement_t(statement_type::create_index, database, collection)
            , index_type_(type) {
        }
        keys_base_storage_t keys_;
        index_type index_type_;
    };
} // namespace components::ql

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<components::ql::create_index_t> final {
                msgpack::object const& operator()(msgpack::object const& o, components::ql::create_index_t& v) const {
                    if (o.type != msgpack::type::ARRAY) {
                        throw msgpack::type_error();
                    }

                    if (o.via.array.size != 4) {
                        throw msgpack::type_error();
                    }

                    auto database = o.via.array.ptr[0].as<std::string>();
                    auto collection = o.via.array.ptr[1].as<std::string>();
                    auto type = static_cast<components::ql::index_type>(o.via.array.ptr[2].as<char>());
                    v = components::ql::create_index_t(database, collection, type);
                    auto data = o.via.array.ptr[3].as<std::vector<std::string>>();
                    //v.keys_ = components::ql::keys_base_storage_t(data.begin(),data.end()); //todo
                    return o;
                }
            };

            template<>
            struct pack<components::ql::create_index_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, components::ql::create_index_t const& v) const {
                    o.pack_array(4);
                    o.pack(v.database_);
                    o.pack(v.collection_);
                    o.pack(static_cast<char>(v.index_type_));
                    o.pack(v.keys_); //todo
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::ql::create_index_t> final {
                void operator()(msgpack::object::with_zone& o, components::ql::create_index_t const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 4;
                    o.via.array.ptr = static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size, MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v.database_, o.zone);
                    o.via.array.ptr[1] = msgpack::object(v.collection_, o.zone);
                    o.via.array.ptr[2] = msgpack::object(static_cast<char>(v.index_type_), o.zone);
                    //std::vector<std::string> tmp(v.keys_.begin(),v.keys_.end());
                    //o.via.array.ptr[3] = msgpack::object(tmp, o.zone); //todo
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
