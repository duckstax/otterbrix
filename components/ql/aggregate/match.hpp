#pragma once

#include "components/ql/experimental/expr.hpp"

namespace components::ql::aggregate {

    using expr_ptr = experimental::expr_ptr;

    struct match_t final {
        expr_ptr query;
    };

    match_t create_match(expr_ptr &&query) {
        match_t match;
        match.query = std::move(query);
        return match;
    }

} // namespace components::ql::aggregate