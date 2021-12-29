#include "parser.hpp"
#include "find_condition.hpp"
#include "document/core/dict.hpp"

using ::document::impl::value_t;
using ::document::impl::dict_t;

namespace components::parser {

condition_type get_condition_(const std::string &key) {
    if (key == "$eq") return condition_type::eq;
    if (key == "$ne") return condition_type::ne;
    if (key == "$gt") return condition_type::gt;
    if (key == "$lt") return condition_type::lt;
    if (key == "$gte") return condition_type::gte;
    if (key == "$lte") return condition_type::lte;
    if (key == "$regex") return condition_type::regex;
    if (key == "$in") return condition_type::any;
    if (key == "$all") return condition_type::all;
    if (key == "$and") return condition_type::union_and;
    if (key == "$or") return condition_type::union_or;
    if (key == "$not") return condition_type::union_not;
    return condition_type::novalid;
}

void parse_find_condition_(find_condition_t *parent_condition, const value_t *condition, const std::string &prev_key, const std::string &key_word);
void parse_find_condition_dict_(find_condition_t *parent_condition, const dict_t *condition, const std::string &prev_key);
void parse_find_condition_array_(find_condition_t *parent_condition, const array_t *condition, const std::string &prev_key);

void parse_find_condition_(find_condition_t *parent_condition, const value_t *condition, const std::string &prev_key, const std::string &key_word) {
    auto real_key = prev_key;
    auto type = get_condition_(key_word);
    if (type == condition_type::novalid) {
        type = get_condition_(prev_key);
        if (type != condition_type::novalid) {
            real_key = key_word;
        }
    }
    auto subcondition = make_find_condition(type, real_key, condition);
    if (subcondition) {
        if (subcondition->is_union()) {
            parse_find_condition_(static_cast<find_condition_t*>(subcondition.get()), condition, real_key, std::string());
        }
        parent_condition->add(std::move(subcondition));
    } else {
        if (condition->type() == value_type::dict) {
            parse_find_condition_dict_(parent_condition, condition->as_dict(), real_key);
        } else if (condition->type() == value_type::array) {
            parse_find_condition_array_(parent_condition, condition->as_array(), real_key);
        }
    }
}

void parse_find_condition_dict_(find_condition_t *parent_condition, const dict_t *condition, const std::string &prev_key) {
    for (auto it = condition->begin(); it; ++it) {
        auto key = std::string(it.key()->as_string());
        if (prev_key.empty()) {
            parse_find_condition_(parent_condition, it.value(), key, std::string());
        } else {
            parse_find_condition_(parent_condition, it.value(), prev_key, key);
        }
    }
}

void parse_find_condition_array_(find_condition_t *parent_condition, const array_t *condition, const std::string &prev_key) {
    for (auto it = condition->begin(); it; ++it) {
        parse_find_condition_(parent_condition, it.value(), prev_key, std::string());
    }
}


find_condition_ptr parse_find_condition(const document_t &condition) {
    auto res_condition = make_condition<find_condition_t>();
    for (auto it = condition.begin(); it; ++it) {
        parse_find_condition_(res_condition.get(), it.value(), std::string(it.key()->as_string()), std::string());
    }
    return res_condition;
}

}