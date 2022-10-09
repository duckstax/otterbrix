#pragma once

#include "components/ql/expr.hpp"
#include "components/ql/expression.hpp"
#include "forward.hpp"

namespace components::ql::aggregate {
    struct group_t final {
        group_t(const expression_t& expression, expr_ptr ptr)
            : expression_(expression)
            , fieldl_(std::move(ptr)) {
        }
        aggregate_types statement = aggregate_types::group;
        expression_t expression_;
        expr_ptr fieldl_;
    };
} // namespace components::ql::aggregate