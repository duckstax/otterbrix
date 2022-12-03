#pragma once

#include <components/ql/aggregate/forward.hpp>
#include <components/ql/experimental/expr.hpp>

namespace components::ql::aggregate {

    using expr_ptr = experimental::expr_ptr;

    struct match_t final {
        static constexpr operator_type type = operator_type::match;
        expr_ptr query;
    };

    match_t make_match(expr_ptr &&query);


    template <class OStream>
    OStream &operator<<(OStream &stream, const match_t &match) {
        stream << "$match: " << match.query;
        return stream;
    }

} // namespace components::ql::aggregate