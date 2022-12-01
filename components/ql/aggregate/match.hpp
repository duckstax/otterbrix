#pragma once

#include <components/ql/aggregate/forward.hpp>
#include <components/expressions/expression.hpp>

namespace components::ql::aggregate {

    struct match_t final {
        static constexpr operator_type type = operator_type::match;
        expressions::expression_ptr query;
    };

    match_t make_match(expressions::expression_ptr&& query);


    template <class OStream>
    OStream &operator<<(OStream& stream, const match_t& match) {
        stream << "$match: " << match.query->to_string();
        return stream;
    }

} // namespace components::ql::aggregate