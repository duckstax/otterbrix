#pragma once

#include "ql_statement.hpp"
#include <components/expressions/key.hpp>
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

    enum class index_compare
    {
        undef,
        str,
        int8,
        int16,
        int32,
        int64,
        uint8,
        uint16,
        uint32,
        uint64,
        float32,
        float64,
        bool8
    };

    inline std::string name_index_compare(index_compare type) {
        switch (type) {
            case index_compare::undef:
                return "undef";
            case index_compare::str:
                return "srt";
            case index_compare::int8:
                return "int8";
            case index_compare::int16:
                return "int16";
            case index_compare::int32:
                return "int32";
            case index_compare::int64:
                return "int64";
            case index_compare::uint8:
                return "uint8";
            case index_compare::uint16:
                return "uint16";
            case index_compare::uint32:
                return "uint32";
            case index_compare::uint64:
                return "uint64";
            case index_compare::float32:
                return "float32";
            case index_compare::float64:
                return "float64";
            case index_compare::bool8:
                return "bool8";
        }
        return "invalid";
    }

    struct create_index_t final : ql_statement_t {
        create_index_t(const database_name_t& database,
                       const collection_name_t& collection,
                       index_type type,
                       index_compare index_compare)
            : ql_statement_t(statement_type::create_index, database, collection)
            , index_type_(type)
            , index_compare_(index_compare) {}

        create_index_t(const database_name_t& database,
                       const collection_name_t& collection,
                       const std::string& name,
                       index_type type = index_type::single)
            : ql_statement_t(statement_type::create_index, database, collection)
            , name_{name}
            , index_type_(type)
            , index_compare_(index_compare::undef) {}

        create_index_t()
            : ql_statement_t(statement_type::create_index, {}, {})
            , index_type_(index_type::no_valid)
            , index_compare_(index_compare::str) {}

        create_index_t(const create_index_t&) = default;
        create_index_t& operator=(const create_index_t&) = default;
        create_index_t(create_index_t&&) = default;
        create_index_t& operator=(create_index_t&&) = default;

        std::string name() const {
            std::stringstream s;
            s << name_ << "__" << collection_ << "_";
            for (const auto& key : keys_) {
                s << "_" << key.as_string();
            }
            return s.str();
        }

        ~create_index_t() final = default;

        std::string name_ = {"unnamed"};
        keys_base_storage_t keys_;
        index_type index_type_;
        index_compare index_compare_;
    };

    struct drop_index_t final : ql_statement_t {
        drop_index_t(const database_name_t& database, const collection_name_t& collection)
            : ql_statement_t(statement_type::drop_index, database, collection) {}

        drop_index_t(const database_name_t& database,
                     const collection_name_t& collection,
                     components::expressions::key_t key)
            : ql_statement_t(statement_type::drop_index, database, collection) {
            keys_.push_back(std::move(key));
        }

        drop_index_t()
            : ql_statement_t(statement_type::drop_index, {}, {}) {}

        std::string name() const {
            std::stringstream s;
            s << collection_ << "_";
            for (const auto& key : keys_) {
                s << "_" << key.as_string();
            }
            return s.str();
        }

        ~drop_index_t() final = default;

        keys_base_storage_t keys_;
    };

} // namespace components::ql

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<components::ql::create_index_t> final {
                msgpack::object const& operator()(msgpack::object const& o, components::ql::create_index_t& v) const {
                    if (o.type != msgpack::type::ARRAY || o.via.array.size != 5) {
                        throw msgpack::type_error();
                    }
                    v.database_ = o.via.array.ptr[0].as<std::string>();
                    v.collection_ = o.via.array.ptr[1].as<std::string>();
                    v.index_type_ = static_cast<components::ql::index_type>(o.via.array.ptr[2].as<uint8_t>());
                    v.index_compare_ = static_cast<components::ql::index_compare>(o.via.array.ptr[3].as<uint8_t>());
                    auto data = o.via.array.ptr[4].as<std::vector<std::string>>();
                    v.keys_ = components::ql::keys_base_storage_t(data.begin(), data.end());
                    return o;
                }
            };

            template<>
            struct pack<components::ql::create_index_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, components::ql::create_index_t const& v) const {
                    o.pack_array(5);
                    o.pack(v.database_);
                    o.pack(v.collection_);
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
                    o.via.array.size = 5;
                    o.via.array.ptr =
                        static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size,
                                                                            MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v.database_, o.zone);
                    o.via.array.ptr[1] = msgpack::object(v.collection_, o.zone);
                    o.via.array.ptr[2] = msgpack::object(static_cast<uint8_t>(v.index_type_), o.zone);
                    o.via.array.ptr[3] = msgpack::object(static_cast<uint8_t>(v.index_compare_), o.zone);
                    std::vector<std::string> tmp(v.keys_.begin(), v.keys_.end());
                    o.via.array.ptr[4] = msgpack::object(tmp, o.zone);
                }
            };

            template<>
            struct convert<components::ql::drop_index_t> final {
                msgpack::object const& operator()(msgpack::object const& o, components::ql::drop_index_t& v) const {
                    if (o.type != msgpack::type::ARRAY || o.via.array.size != 3) {
                        throw msgpack::type_error();
                    }
                    v.database_ = o.via.array.ptr[0].as<std::string>();
                    v.collection_ = o.via.array.ptr[1].as<std::string>();
                    auto data = o.via.array.ptr[2].as<std::vector<std::string>>();
                    v.keys_ = components::ql::keys_base_storage_t(data.begin(), data.end());
                    return o;
                }
            };

            template<>
            struct pack<components::ql::drop_index_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, components::ql::drop_index_t const& v) const {
                    o.pack_array(3);
                    o.pack(v.database_);
                    o.pack(v.collection_);
                    o.pack(v.keys_);
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::ql::drop_index_t> final {
                void operator()(msgpack::object::with_zone& o, components::ql::drop_index_t const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 3;
                    o.via.array.ptr =
                        static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size,
                                                                            MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v.database_, o.zone);
                    o.via.array.ptr[1] = msgpack::object(v.collection_, o.zone);
                    std::vector<std::string> tmp(v.keys_.begin(), v.keys_.end());
                    o.via.array.ptr[2] = msgpack::object(tmp, o.zone);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
