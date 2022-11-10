#pragma once

#include "expr.hpp"

namespace components::ql::experimental {

    enum class project_expr_type : std::uint8_t {
        invalid,

        get_field,

        count,
        sum,
        min,
        max,
        avg,

        add,
        subtract,
        multiply,
        divide,

        round,
        ceil,
        floor,

        abs,
        mod,
        pow,
        sqrt
    };


    struct project_expr_t {
        project_expr_t(const expr_t&) = delete;
        project_expr_t(project_expr_t&&) = default;

        using ptr = std::unique_ptr<project_expr_t>;
        using param_storage = std::variant<
            core::parameter_id_t,
            key_t,
            ptr>;

        project_expr_type type;
        key_t key;
        std::vector<param_storage> params;

        project_expr_t(project_expr_type type, key_t key);
        void append_param(param_storage param);
    };

    using project_expr_ptr = project_expr_t::ptr;

    project_expr_ptr make_project_expr(project_expr_type type, const key_t& key);
    project_expr_ptr make_project_expr(project_expr_type type);

    project_expr_type get_project_type(const std::string& key);

    std::string to_string(const project_expr_ptr& expr);

} // namespace components::ql::experiment