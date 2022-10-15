#pragma once

#include "components/ql/expr.hpp"
#include "components/ql/expression.hpp"
#include "forward.hpp"

namespace components::ql::aggregate {
    struct match_t final {
        operator_type statement = operator_type::match;
        expr_ptr query_;
    };
} // namespace components::ql::aggregate