#pragma once

#include <components/ql/aggregate/forward.hpp>
#include <components/ql/experimental/expr.hpp>
#include <components/ql/expression.hpp>

namespace components::ql::aggregate {

    using expr_ptr = experimental::expr_ptr;

    struct group_t final {
        static constexpr operator_type type = operator_type::group;
        expr_ptr fields;
    };

    group_t make_group(expr_ptr &&query);

#ifdef DEV_MODE
    std::string debug(const group_t &group);
#endif

} // namespace components::ql::aggregate