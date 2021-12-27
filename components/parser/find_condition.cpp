#include "find_condition.hpp"

namespace components::parser {

bool find_condition_and::check_document(const document_view_t &doc) const {
    return check_document_(doc);
}

bool find_condition_and::check_document(const document_t &doc) const {
    return check_document_(doc);
}

template<class T> bool find_condition_and::check_document_(const T &doc) const {
    for (const auto &condition : conditions_) {
        if (!condition->is_fit(doc)) {
            return false;
        }
    }
    return true;
}


bool find_condition_or::check_document(const document_view_t &doc) const {
    return check_document_(doc);
}

bool find_condition_or::check_document(const document_t &doc) const {
    return check_document_(doc);
}

template<class T> bool find_condition_or::check_document_(const T &doc) const {
    for (const auto &condition : conditions_) {
        if (condition->is_fit(doc)) {
            return true;
        }
    }
    return false;
}


bool find_condition_not::check_document(const document_view_t &doc) const {
    return check_document_(doc);
}

bool find_condition_not::check_document(const document_t &doc) const {
    return check_document_(doc);
}

template<class T> bool find_condition_not::check_document_(const T &doc) const {
    for (const auto &condition : conditions_) {
        if (condition->is_fit(doc)) {
            return false;
        }
    }
    return true;
}


#define MAKE_FIND_CONDITION(CONDITION, KEY, VALUE) \
    (VALUE->type() == value_type::boolean) ? static_cast<conditional_expression_ptr>(make_condition<CONDITION<bool>>(KEY)) : \
    (VALUE->is_unsigned()) ? static_cast<conditional_expression_ptr>(make_condition<CONDITION<uint64_t>>(KEY)) : \
    (VALUE->is_int()) ? static_cast<conditional_expression_ptr>(make_condition<CONDITION<int64_t>>(KEY)) : \
    (VALUE->is_double()) ? static_cast<conditional_expression_ptr>(make_condition<CONDITION<double>>(KEY)) : \
    (VALUE->type() == value_type::string) ? static_cast<conditional_expression_ptr>(make_condition<CONDITION<std::string>>(KEY)) : \
    nullptr

conditional_expression_ptr make_find_condition(condition_type type, const std::string &key, const value_t *value) {
    conditional_expression_ptr condition = nullptr;
    switch (type) {
    case condition_type::novalid:
        if (key.empty()) return nullptr;
        condition = MAKE_FIND_CONDITION(find_condition_eq, key, value);
        break;
    case condition_type::eq:
        condition = MAKE_FIND_CONDITION(find_condition_eq, key, value);
        break;
    case condition_type::ne:
        condition = MAKE_FIND_CONDITION(find_condition_ne, key, value);
        break;
    case condition_type::gt:
        condition = MAKE_FIND_CONDITION(find_condition_gt, key, value);
        break;
    case condition_type::lt:
        condition = MAKE_FIND_CONDITION(find_condition_lt, key, value);
        break;
    case condition_type::gte:
        condition = MAKE_FIND_CONDITION(find_condition_gte, key, value);
        break;
    case condition_type::lte:
        condition = MAKE_FIND_CONDITION(find_condition_lte, key, value);
        break;
    case condition_type::regex:
        return make_condition<find_condition_regex>(key, static_cast<std::string>(value->as_string()));
        break;
    case condition_type::any:
        condition = value->type() == value_type::array
                ? MAKE_FIND_CONDITION(find_condition_any, key, value->as_array()->get(0))
                : MAKE_FIND_CONDITION(find_condition_any, key, value);
        break;
    case condition_type::all:
        condition = value->type() == value_type::array
                ? MAKE_FIND_CONDITION(find_condition_all, key, value->as_array()->get(0))
                : MAKE_FIND_CONDITION(find_condition_all, key, value);
        break;
    case condition_type::union_and:
        return make_condition<find_condition_and>();
        break;
    case condition_type::union_or:
        return make_condition<find_condition_or>();
        break;
    case condition_type::union_not:
        return make_condition<find_condition_not>();
        break;
    default:
        return nullptr;
    }
    if (condition) {
        condition->set_value(value);
    }
    return condition;
}

}