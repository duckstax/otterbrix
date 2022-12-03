#pragma once

#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <components/document/mutable/mutable_value.hpp>
#include <components/document/wrapper_value.hpp>
#include <components/document/document.hpp>

#include "key.hpp"
#include "predicate.hpp"

namespace components::ql {


    bool is_union_condition(condition_type type);

    using expr_value_t = ::document::wrapper_value_t;

    struct expr_t {
        expr_t(const expr_t&) = delete;
        expr_t(expr_t&&) /*noexcept(false)*/ = default;

        using ptr = std::unique_ptr<expr_t>;

        condition_type type_;
        key_t key_;
        expr_value_t value_;
        std::vector<ptr> sub_conditions_;

        expr_t(condition_type type, std::string key, expr_value_t value);
        explicit expr_t(bool is_union);
        bool is_union() const;
        void append_sub_condition(ptr sub_condition);

    private:
        bool union_;
    };

    using expr_ptr = expr_t::ptr;

    template<class Value>
    inline expr_ptr make_expr(condition_type condition, std::string key, Value value) {
        return make_expr(condition, std::move(key), ::document::impl::new_value(value).detach());
    }

    template<>
    inline expr_ptr make_expr(condition_type condition, std::string key, expr_value_t value) {
        return std::make_unique<expr_t>(condition, std::move(key), value);
    }

    template<>
    inline expr_ptr make_expr(condition_type condition, std::string key, const ::document::impl::value_t* value) {
        return make_expr(condition, std::move(key), expr_value_t(value));
    }

    template<>
    inline expr_ptr make_expr(condition_type condition, std::string key, const std::string& value) {
        return make_expr(condition, std::move(key), ::document::impl::new_value(value).detach());
    }

    inline expr_ptr make_expr() {
        return std::make_unique<expr_t>(false);
    }

    inline expr_ptr make_union_expr() {
        return std::make_unique<expr_t>(true);
    }

    std::string to_string(condition_type type);
    std::string to_string(const expr_ptr& expr);

    ::document::wrapper_value_t get_value(const components::document::document_ptr &doc, const key_t &key);

} // namespace components::ql