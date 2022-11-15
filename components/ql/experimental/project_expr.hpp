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


    template <class OStream>
    OStream &operator<<(OStream &stream, const project_expr_t::param_storage &param) {
        std::visit([&stream](const auto &p) {
            using type = std::decay_t<decltype(p)>;
            if constexpr (std::is_same_v<type, core::parameter_id_t>) {
                stream << "#" << p;
            } else if constexpr (std::is_same_v<type, key_t>) {
                stream << "\"$" << p << "\"";
            } else if constexpr (std::is_same_v<type, project_expr_ptr>) {
                stream << p;
            }
        }, param);
        return stream;
    }


    template <class OStream>
    OStream &operator<<(OStream &stream, const project_expr_ptr &expr) {
        if (expr->params.empty()) {
            stream << expr->key;
        } else {
            if (!expr->key.is_null()) {
                stream << expr->key << ": ";
            }
            if (expr->type != project_expr_type::get_field) {
                stream << "{$" << magic_enum::enum_name(expr->type) << ": ";
            }
            if (expr->params.size() > 1) {
                stream << "[";
                bool is_first = true;
                for (const auto& param : expr->params) {
                    if (is_first) {
                        is_first = false;
                    } else {
                        stream << ", ";
                    }
                    stream << param;
                }
                stream << "]";
            } else {
                stream << expr->params.at(0);
            }
            if (expr->type != project_expr_type::get_field) {
                stream << "}";
            }
        }
        return stream;
    }

} // namespace components::ql::experiment