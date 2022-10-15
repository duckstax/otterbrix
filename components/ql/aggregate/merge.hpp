#pragma once

#include "components/ql/expr.hpp"
#include "components/ql/expression.hpp"
#include "forward.hpp"

namespace components::ql::aggregate {
    struct merge_t final {
        operator_type statement = operator_type::merge;
        std::string into;
    };
} // namespace components::ql::aggregate