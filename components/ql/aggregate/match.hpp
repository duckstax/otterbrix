#pragma once

#include "components/ql/expr.hpp"
#include "components/ql/expression.hpp"
#include "forward.hpp"

namespace components::ql::aggregate {
    struct match_t final {
        aggregate_types statement = aggregate_types::match;
        expr_ptr query_;
    };
} // namespace components::ql::aggregate