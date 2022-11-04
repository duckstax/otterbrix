#pragma once
#include "expr.hpp"

namespace components::ql::experimental {

    struct project_expr_t {
        project_expr_t(const expr_t&) = delete;
        project_expr_t(project_expr_t&&) = default;

        using ptr = std::unique_ptr<project_expr_t>;
        using param_storage = std::variant<
            core::parameter_id_t,
            key_t,
            ptr>;

        //project_expr_type type_;
        key_t key_;
        std::vector<param_storage> params_;

//        expr_t(project_expr_type type, key_t key);
//        void append_param(const param_storage& param);
    };

//    using project_expr_ptr = project_expr_t::ptr;

//    project_expr_ptr make_expr(project_expr_type type, const key_t& key);

//    std::string to_string(project_expr_type type);
//    std::string to_string(const project_expr_t& expr);

} // namespace components::ql::experiment