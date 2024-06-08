#pragma once

#include "ql_statement.hpp"
#include <components/expressions/key.hpp>
#include <core/types.hpp>
#include <memory_resource>
#include <msgpack.hpp>
#include <vector>

namespace components::ql {

    using keys_base_storage_t = std::pmr::vector<components::expressions::key_t>;

    enum class index_type : uint8_t
    {
        single,
        composite,
        multikey,
        hashed,
        wildcard,
        no_valid = 255
    };

    inline std::string name_index_type(index_type type) {
        switch (type) {
            case index_type::single:
                return "single";
            case index_type::composite:
                return "composite";
            case index_type::multikey:
                return "multikey";
            case index_type::hashed:
                return "hashed";
            case index_type::wildcard:
                return "wildcard";
            case index_type::no_valid:
                return "no_valid";
        }
        return "default";
    }

    struct create_index_t final : ql_statement_t {
        create_index_t(const database_name_t& database,
                       const collection_name_t& collection,
                       const std::string& name,
                       index_type type,
                       core::type index_compare)
            : ql_statement_t(statement_type::create_index, database, collection)
            , name_{name}
            , index_type_(type)
            , index_compare_(index_compare) {}

        create_index_t(const database_name_t& database,
                       const collection_name_t& collection,
                       const std::string& name,
                       index_type type = index_type::single)
            : ql_statement_t(statement_type::create_index, database, collection)
            , name_{name}
            , index_type_(type)
            , index_compare_(core::type::undef) {}

        create_index_t()
            : ql_statement_t(statement_type::create_index, {}, {})
            , index_type_(index_type::no_valid)
            , index_compare_(core::type::str) {}

        create_index_t(const create_index_t&) = default;
        create_index_t& operator=(const create_index_t&) = default;
        create_index_t(create_index_t&&) = default;
        create_index_t& operator=(create_index_t&&) = default;

        std::string name() const {
            std::stringstream s;
            s << collection_ << "_" << name_;
            return s.str();
        }

        ~create_index_t() final = default;

        std::string name_ = {"unnamed"};
        keys_base_storage_t keys_;
        index_type index_type_;
        core::type index_compare_;
    };

    struct drop_index_t final : ql_statement_t {
        drop_index_t(const database_name_t& database, const collection_name_t& collection, const std::string& name)
            : ql_statement_t(statement_type::drop_index, database, collection)
            , name_{name} {}

        drop_index_t()
            : ql_statement_t(statement_type::drop_index, {}, {}) {}

        std::string name() const {
            std::stringstream s;
            s << collection_ << "_" << name_;
            return s.str();
        }

        ~drop_index_t() final = default;

        std::string name_;
    };

    using create_index_ptr = std::unique_ptr<create_index_t>;

} // namespace components::ql

// User defined class template specialization
namespace msgpack {
    constexpr uint32_t CREATE_INDEX_SIZE = 6;
    constexpr uint32_t DROP_INDEX_SIZE = 3;
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<components::ql::create_index_t> final {
                msgpack::object const& operator()(msgpack::object const& o, components::ql::create_index_t& v) const {
                    if (o.type != msgpack::type::ARRAY || o.via.array.size != CREATE_INDEX_SIZE) {
                        throw msgpack::type_error();
                    }
                    v.database_ = o.via.array.ptr[0].as<std::string>();
                    v.collection_ = o.via.array.ptr[1].as<std::string>();
                    v.name_ = o.via.array.ptr[2].as<std::string>();
                    v.index_type_ = static_cast<components::ql::index_type>(o.via.array.ptr[3].as<uint8_t>());
                    v.index_compare_ = static_cast<core::type>(o.via.array.ptr[4].as<uint8_t>());
                    auto data = o.via.array.ptr[5].as<std::vector<std::string>>();
                    v.keys_ = components::ql::keys_base_storage_t(data.begin(), data.end());
                    return o;
                }
            };

            template<>
            struct pack<components::ql::create_index_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, components::ql::create_index_t const& v) const {
                    o.pack_array(CREATE_INDEX_SIZE);
                    o.pack(v.database_);
                    o.pack(v.collection_);
                    o.pack(v.name_);
                    o.pack(static_cast<uint8_t>(v.index_type_));
                    o.pack(static_cast<uint8_t>(v.index_compare_));
                    o.pack(v.keys_);
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::ql::create_index_t> final {
                void operator()(msgpack::object::with_zone& o, components::ql::create_index_t const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = CREATE_INDEX_SIZE;
                    o.via.array.ptr =
                        static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size,
                                                                            MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v.database_, o.zone);
                    o.via.array.ptr[1] = msgpack::object(v.collection_, o.zone);
                    o.via.array.ptr[2] = msgpack::object(v.name_, o.zone);
                    o.via.array.ptr[3] = msgpack::object(static_cast<uint8_t>(v.index_type_), o.zone);
                    o.via.array.ptr[4] = msgpack::object(static_cast<uint8_t>(v.index_compare_), o.zone);
                    std::vector<std::string> tmp(v.keys_.begin(), v.keys_.end());
                    o.via.array.ptr[5] = msgpack::object(tmp, o.zone);
                }
            };

            template<>
            struct convert<components::ql::drop_index_t> final {
                msgpack::object const& operator()(msgpack::object const& o, components::ql::drop_index_t& v) const {
                    if (o.type != msgpack::type::ARRAY || o.via.array.size != DROP_INDEX_SIZE) {
                        throw msgpack::type_error();
                    }
                    v.database_ = o.via.array.ptr[0].as<std::string>();
                    v.collection_ = o.via.array.ptr[1].as<std::string>();
                    v.name_ = o.via.array.ptr[2].as<std::string>();

                    return o;
                }
            };

            template<>
            struct pack<components::ql::drop_index_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, components::ql::drop_index_t const& v) const {
                    o.pack_array(DROP_INDEX_SIZE);
                    o.pack(v.database_);
                    o.pack(v.collection_);
                    o.pack(v.name_);
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::ql::drop_index_t> final {
                void operator()(msgpack::object::with_zone& o, components::ql::drop_index_t const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = DROP_INDEX_SIZE;
                    o.via.array.ptr =
                        static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size,
                                                                            MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v.database_, o.zone);
                    o.via.array.ptr[1] = msgpack::object(v.collection_, o.zone);
                    o.via.array.ptr[2] = msgpack::object(v.name_, o.zone);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
