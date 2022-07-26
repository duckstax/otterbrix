#include "parser.hpp"
#include "document/core/dict.hpp"
#include <iostream> //todo: delete

using ::document::impl::value_t;
using ::document::impl::value_type;
using ::document::impl::array_t;
using ::document::impl::dict_t;

namespace components::ql {

    using document::document_ptr;
    using document::document_t;
    using document::document_view_t;

    condition_type get_condition_(const std::string& key) {
        if (key == "$eq")
            return condition_type::eq;
        if (key == "$ne")
            return condition_type::ne;
        if (key == "$gt")
            return condition_type::gt;
        if (key == "$lt")
            return condition_type::lt;
        if (key == "$gte")
            return condition_type::gte;
        if (key == "$lte")
            return condition_type::lte;
        if (key == "$regex")
            return condition_type::regex;
        if (key == "$in")
            return condition_type::any;
        if (key == "$all")
            return condition_type::all;
        if (key == "$and")
            return condition_type::union_and;
        if (key == "$or")
            return condition_type::union_or;
        if (key == "$not")
            return condition_type::union_not;
        return condition_type::novalid;
    }

    field_t smart_cast(const value_t* value) {
        if (value->type() == value_type::boolean) {
            return field_t(value->as_bool());
        } else if (value->is_unsigned()) {
            return field_t(value->as_unsigned());
        } else if (value->is_int()) {
            return field_t(value->as_int());
        } else if (value->is_double()) {
            return field_t(value->as_double());
        } else if (value->type() == value_type::string) {
            return field_t(std::string(value->as_string()));
        }
        return field_t();
    }

    void parse_find_condition_(expr_t* parent_condition, const value_t* condition, const std::string& prev_key, const std::string& key_word);
    void parse_find_condition_dict_(expr_t* parent_condition, const dict_t* condition, const std::string& prev_key);
    void parse_find_condition_array_(expr_t* parent_condition, const array_t* condition, const std::string& prev_key);

    void parse_find_condition_(expr_t* parent_condition, const value_t* condition, const std::string& prev_key, const std::string& key_word) {
        auto real_key = prev_key;
        auto type = get_condition_(key_word);
        if (type == condition_type::novalid) {
            type = get_condition_(prev_key);
            if (type != condition_type::novalid) {
                real_key = key_word;
            }
        }
        if (condition->type() == value_type::dict) {
            parse_find_condition_dict_(parent_condition, condition->as_dict(), real_key);
        } else if (condition->type() == value_type::array) {
            parse_find_condition_array_(parent_condition, condition->as_array(), real_key);
        } else {
            auto sub_condition = make_expr(type, real_key, smart_cast(condition));
            if (sub_condition->is_union()) {
                parse_find_condition_(sub_condition.get(), condition, real_key, std::string());
            }
            parent_condition->append_sub_condition(std::move(sub_condition));
        }
    }

    void parse_find_condition_dict_(expr_t* parent_condition, const dict_t* condition, const std::string& prev_key) {
        for (auto it = condition->begin(); it; ++it) {
            auto key = std::string(it.key()->as_string());
            if (prev_key.empty()) {
                parse_find_condition_(parent_condition, it.value(), key, std::string());
            } else {
                parse_find_condition_(parent_condition, it.value(), prev_key, key);
            }
        }
    }

    void parse_find_condition_array_(expr_t* parent_condition, const array_t* condition, const std::string& prev_key) {
        for (auto it = condition->begin(); it; ++it) {
            parse_find_condition_(parent_condition, it.value(), prev_key, std::string());
        }
    }

    expr_ptr parse_find_condition_(const ::document::retained_t<::document::impl::dict_t>& condition) {
        auto res_condition = make_union_expr();
        for (auto it = condition->begin(); it; ++it) {
            if (condition->count() == 1) {
                res_condition->condition_ = get_condition_(it.key_string().as_string());
            }
            parse_find_condition_(res_condition.get(), it.value(), std::string(it.key()->as_string()), std::string());
        }
        return res_condition;
    }

    expr_ptr parse_find_condition(const document_view_t& condition) {
        return parse_find_condition_(condition.to_dict());
    }

    expr_ptr parse_find_condition(const document_ptr& condition) {
        return parse_find_condition(document_view_t(condition));
    }

} // namespace components::ql