#include "project_expr.hpp"
#include <magic_enum.hpp>

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

    std::string to_string(project_expr_type type) {
        return "$" + static_cast<std::string>(magic_enum::enum_name(type));
    }

    std::string to_string(const project_expr_t::param_storage &param) {
        return std::visit([](const auto &p) {
            using type = std::decay_t<decltype(p)>;
            if constexpr (std::is_same_v<type, core::parameter_id_t>) {
                return std::string("#" + std::to_string(p));
            } else if constexpr (std::is_same_v<type, key_t>) {
                return std::string("\"$" + p.as_string() + "\"");
            } else if constexpr (std::is_same_v<type, project_expr_ptr>) {
                return to_string(p);
            }
        }, param);
    }

    std::string to_string(const project_expr_ptr& expr) {
        if (expr->params.empty()) {
            return expr->key.as_string();
        }
        std::string value;
        if (!expr->key.is_null()) {
            value += expr->key.as_string() + ": ";
        }
        if (expr->type != project_expr_type::get_field) {
            value += "{" + to_string(expr->type) + ": ";
        }
        if (expr->params.size() > 1) {
            value += "[";
            bool is_first = true;
            for (const auto& param : expr->params) {
                if (is_first) {
                    is_first = false;
                } else {
                    value += ", ";
                }
                value += to_string(param);
            }
            value += "]";
        } else {
            value += to_string(expr->params.at(0));
        }
        if (expr->type != project_expr_type::get_field) {
            value += "}";
        }
        return value;
    }

} // namespace components::ql::experiment