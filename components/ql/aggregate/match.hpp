#pragma once

#include <components/expressions/expression.hpp>
#include <components/expressions/msgpack.hpp>
#include <components/ql/aggregate/forward.hpp>

namespace components::ql::aggregate {

    struct match_t final {
        static constexpr operator_type type = operator_type::match;
        expressions::expression_ptr query{nullptr};
    };

    match_t make_match(expressions::expression_ptr&& query);

    template<class OStream>
    OStream& operator<<(OStream& stream, const match_t& match) {
        stream << "$match: {" << match.query->to_string() << "}";
        return stream;
    }

    inline match_t to_match(const msgpack::object& msg_object, std::pmr::memory_resource* resource) {
        match_t result;
        result.query = expressions::to_expression(msg_object, resource);
        return result;
    }

} // namespace components::ql::aggregate

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct pack<components::ql::aggregate::match_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o,
                                           components::ql::aggregate::match_t const& v) const {
                    o.pack(v.query);
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::ql::aggregate::match_t> final {
                void operator()(msgpack::object::with_zone& o, components::ql::aggregate::match_t const& v) const {
                    msgpack::object(v.query, o.zone);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
