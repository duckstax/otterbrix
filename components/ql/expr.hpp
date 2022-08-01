#pragma once

#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <core/field/field.hpp>

namespace components::ql {

    enum class condition_type : std::uint8_t {
        novalid,
        eq,
        ne,
        gt,
        lt,
        gte,
        lte,
        regex,
        any,
        all,
        union_and,
        union_or,
        union_not
    };

    struct expr_t {
        condition_type condition_;
        std::string key_;
        Field field_;
        std::vector<expr_t> fields_;
    };

    using expr_ptr = expr_t*;

    template<class Value>
    expr_ptr make_expr_eq(const std::string& key, Value value) {
        auto* expr = new expr_t;

        expr->condition_ = condition_type::eq;
        expr->key_ = key;
        expr->field_ = value;

        return expr;
    }

    template<class Value>
    expr_ptr make_expr_ne(const std::string& key, Value value) {
        auto* expr = new expr_t;

        expr->condition_ = condition_type::ne;
        expr->key_ = key;
        expr->field_ = value;

        return expr;
    }

    template<class Value>
    expr_ptr make_expr_gt(const std::string& key, Value value) {
        auto* expr = new expr_t;

        expr->condition_ = condition_type::gt;
        expr->key_ = key;
        expr->field_ = value;

        return expr;
    }

    template<class Value>
    expr_ptr make_expr_lt(const std::string& key, Value value) {
        auto* expr = new expr_t;

        expr->condition_ = condition_type::lt;
        expr->key_ = key;
        expr->field_ = value;

        return expr;
    }

    template<class Value>
    expr_ptr make_expr_gte(const std::string& key, Value value) {
        auto* expr = new expr_t;

        expr->condition_ = condition_type::gte;
        expr->key_ = key;
        expr->field_ = value;

        return expr;
    }

    template<class Value>
    expr_ptr make_expr_lte(const std::string& key, Value value) {
        auto* expr = new expr_t;

        expr->condition_ = condition_type::lte;
        expr->key_ = key;
        expr->field_ = value;

        return expr;
    }

    expr_ptr make_expr_regex(const std::string& key, const std::string& value) {
        auto* expr = new expr_t;

        expr->condition_ = condition_type::regex;
        expr->key_ = key;
        expr->field_ = value;

        return expr;
    }

    template<class Value>
    expr_ptr make_expr_any(const std::string& key, Value value) {
        auto* expr = new expr_t;

        expr->condition_ = condition_type::any;
        expr->key_ = key;
        expr->field_ = value;

        return expr;
    }

    template<class Value>
    expr_ptr make_expr_and(const std::string& key, Value value) {
        auto* expr = new expr_t;

        expr->condition_ = condition_type::union_and;
        expr->key_ = key;
        expr->field_ = value;

        return expr;
    }

    template<class Value>
    expr_ptr make_expr_union_or(const std::string& key, Value value) {
        auto* expr = new expr_t;

        expr->condition_ = condition_type::union_or;
        expr->key_ = key;
        expr->field_ = value;

        return expr;
    }

    template<class Value>
    expr_ptr make_expr_union_not(const std::string& key, Value value) {
        auto* expr = new expr_t;

        expr->condition_ = condition_type::union_not;
        expr->key_ = key;
        expr->field_ = value;

        return expr;
    }
} // namespace components::ql