#pragma once

#include <components/expressions/expression.hpp>
#include <components/ql/aggregate.hpp>
#include <components/ql/ql_statement.hpp>
#include "join_types.h"

namespace components::ql {

    struct join_t final : ql_base_statement_t {
        join_type type {join_type::inner};
        aggregate_statement_ptr left;
        aggregate_statement_ptr right;
        std::vector<expressions::expression_ptr> expressions;

        join_t() = default;
        explicit join_t(join_type type);
    };

} // namespace components::ql
