#include "project_expr.hpp"

namespace components::ql::experimental {

    project_expr_t::project_expr_t(project_expr_type type, key_t key)
        : type(type)
        , key(std::move(key)) {
    }

    void project_expr_t::append_param(project_expr_t::param_storage param) {
        params.push_back(std::move(param));
    }

    project_expr_ptr make_project_expr(project_expr_type type, const key_t& key) {
        return std::make_unique<project_expr_t>(type, key);
    }

    project_expr_ptr make_project_expr(project_expr_type type) {
        return make_project_expr(type, key_t());
    }

    project_expr_type get_project_type(const std::string& key) {
        auto type = magic_enum::enum_cast<project_expr_type>(key);
        if (type.has_value()) {
            return type.value();
        }
        return project_expr_type::invalid;
    }

} // namespace components::ql::experiment