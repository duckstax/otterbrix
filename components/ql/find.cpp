#include "find.hpp"

#define MAKE_FIND_CONDITION(CONDITION, KEY, VALUE) \
    (VALUE->type() == value_type::boolean) ? static_cast<conditional_expression_ptr>(make_condition<CONDITION<bool>>(KEY)) : \
    (VALUE->is_unsigned()) ? static_cast<conditional_expression_ptr>(make_condition<CONDITION<uint64_t>>(KEY)) : \
    (VALUE->is_int()) ? static_cast<conditional_expression_ptr>(make_condition<CONDITION<int64_t>>(KEY)) : \
    (VALUE->is_double()) ? static_cast<conditional_expression_ptr>(make_condition<CONDITION<double>>(KEY)) : \
    (VALUE->type() == value_type::string) ? static_cast<conditional_expression_ptr>(make_condition<CONDITION<std::string>>(KEY)) : \
    nullptr

condition_ptr make_find_condition(condition_type type, const std::string &key, const value_t *value) {
    condition_ptr condition = nullptr;
    switch (type) {
        case condition_type::novalid:
            if (key.empty()) return nullptr;
            condition = MAKE_FIND_CONDITION(find_condition_eq_ne_gt_lt_gte_lte, key, value);
            break;
        case condition_type::eq:
            condition = MAKE_FIND_CONDITION(find_condition_eq_ne_gt_lt_gte_lte, key, value);
            break;
        case condition_type::ne:
            condition = MAKE_FIND_CONDITION(find_condition_eq_ne_gt_lt_gte_lte, key, value);
            break;
        case condition_type::gt:
            condition = MAKE_FIND_CONDITION(find_condition_eq_ne_gt_lt_gte_lte, key, value);
            break;
        case condition_type::lt:
            condition = MAKE_FIND_CONDITION(find_condition_eq_ne_gt_lt_gte_lte, key, value);
            break;
        case condition_type::gte:
            condition = MAKE_FIND_CONDITION(find_condition_eq_ne_gt_lt_gte_lte, key, value);
            break;
        case condition_type::lte:
            condition = MAKE_FIND_CONDITION(find_condition_eq_ne_gt_lt_gte_lte, key, value);
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
