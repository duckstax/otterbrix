#pragma once

#include "components/ql/experimental/expr.hpp"

namespace components::ql::aggregate {

    using expr_ptr = experimental::expr_ptr;

    struct match_t final {
        //static constexpr operator_type type = operator_type::match;
        expr_ptr query;
    };

    match_t make_match(expr_ptr &&query);

#ifdef DEV_MODE
    std::string debug(const match_t &match);
#endif

} // namespace components::ql::aggregate