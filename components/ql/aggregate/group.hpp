#pragma once

#include <components/ql/aggregate/forward.hpp>
#include <components/ql/experimental/project_expr.hpp>

namespace components::ql::aggregate {

    using project_expr_ptr = experimental::project_expr_ptr;

    struct group_t final {
        static constexpr operator_type type = operator_type::group;
        std::vector<project_expr_ptr> fields;
    };

#ifdef DEV_MODE
    std::string debug(const group_t &group);
#endif

} // namespace components::ql::aggregate