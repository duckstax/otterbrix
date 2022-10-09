#pragma once

#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <components/document/mutable/mutable_value.hpp>
#include <components/document/wrapper_value.hpp>

#include "key.hpp"
#include "predicate.hpp"

#include "core/strong_typedef.hpp"

STRONG_TYPEDEF(uint16_t, parameter_id_t);

namespace components::ql::experiment {

    bool is_union_condition(condition_type type);

    using expr_value_t = ::document::wrapper_value_t;

    struct expr_t {
        expr_t(const expr_t&) = delete;
        expr_t(expr_t&&) /*noexcept(false)*/ = default;

        using ptr = std::unique_ptr<expr_t>;

        condition_type type_;
        key_t key_;
        core::parameter_id_t value_;
        std::vector<ptr> sub_conditions_;

        expr_t(condition_type type, std::string key, core::parameter_id_t);
        explicit expr_t(bool is_union);
        bool is_union() const;
        void append_sub_condition(ptr sub_condition);

    private:
        bool union_;
    };

    using expr_ptr = expr_t::ptr;

    expr_ptr make_expr(condition_type condition, const std::string& key, core::parameter_id_t id);
    expr_ptr make_expr();
    expr_ptr make_union_expr();

    std::string to_string(condition_type type);
    std::string to_string(const expr_ptr& expr);

} // namespace components::ql::experiment