#pragma once

#include <msgpack.hpp>
#include <msgpack/zone.hpp>
#include <msgpack/adaptor/list.hpp>
#include "components/ql/ql_statement.hpp"

namespace components::ql {

    struct create_database_t final : ql_statement_t {
        explicit create_database_t(const database_name_t& database);
        create_database_t() = default;
        create_database_t(const create_database_t&) = default;
        create_database_t& operator=(const create_database_t&) = default;
        create_database_t(create_database_t&&) = default;
        create_database_t& operator=(create_database_t&&) = default;
        ~create_database_t() final = default;
    };

} // namespace components::protocol

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<components::ql::create_database_t> final {
                msgpack::object const& operator()(msgpack::object const& o, components::ql::create_database_t& v) const {
                    if (o.type != msgpack::type::ARRAY) {
                        throw msgpack::type_error();
                    }
                    if (o.via.array.size != 1) {
                        throw msgpack::type_error();
                   }
                    v.database_ = o.via.array.ptr[0].as<components::ql::database_name_t>();
                    return o;
                }
            };

            template<>
            struct pack<components::ql::create_database_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, components::ql::create_database_t const& v) const {
                    o.pack_array(1);
                    o.pack(v.database_);
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::ql::create_database_t> final {
                void operator()(msgpack::object::with_zone& o, components::ql::create_database_t const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 1;
                    o.via.array.ptr = static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size, MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v.database_, o.zone);
                }
            };

        } // namespace adaptor
    } // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
