#pragma once

#include "components/ql/expr.hpp"
#include "components/ql/expression.hpp"
#include "forward.hpp"

namespace components::ql::aggregate {
    struct merge_t final {
        //static constexpr operator_type type = operator_type::merge;
        std::string into;
    };
} // namespace components::ql::aggregate