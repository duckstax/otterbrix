#include "find.hpp"

using ::document::impl::array_t;
using ::document::impl::value_t;
using ::document::impl::value_type;

namespace components::ql {



    void smart_cast(const value_t* value, Field& field) {
        if (value->() == value_type::boolean) {
            field(value->as_bool());
            return;
        }

        if (value->is_unsigned()) {
            field(value->as_unsigned());
            return;
        }

        if (value->is_int()) {
            field(value->as_int());
            return;
        }

        if (value->is_double()) {
            field(value->as_double());
            return;
        }

        if (value->type() == value_type::string) {
            field(value->as_string());
            return;
        }
    }

    expr_ptr make_find_condition(condition_type type, const std::string& key, const value_t* value) {
        expr_ptr condition = nullptr;
        Field field;
        smart_cast(value, field);
        switch (type) {
            case condition_type::novalid:
                if (key.empty()) {
                    return nullptr;
                }
                //condition = MAKE_FIND_CONDITION(expr_ptr, key, smart_cast);
                break;
            case condition_type::eq:
                condition = make_expr_eq(key, field);
                break;
            case condition_type::ne:
                condition = make_expr_ne(key, field);
                break;
            case condition_type::gt:
                condition = make_expr_gt(key, field);
                break;
            case condition_type::lt:
                condition = make_expr_lt(key, field);
                break;
            case condition_type::gte:
                condition = make_expr_gte(key, field);
                break;
            case condition_type::lte:
                condition = make_expr_lte(key, field);
                break;
            case condition_type::regex:
                return make_expr_regex(key, field);
                break;
            case condition_type::any:
                condition = value->type() == value_type::array
                                ? MAKE_FIND_CONDITION(expr_ptr, key, value->as_array()->get(0))
                                : MAKE_FIND_CONDITION(expr_ptr, key, value);
                break;
            case condition_type::all:
                condition = value->type() == value_type::array
                                ? MAKE_FIND_CONDITION(expr_ptr, key, value->as_array()->get(0))
                                : MAKE_FIND_CONDITION(expr_ptr, key, value);
                break;
            case condition_type::union_and:
                return make_expr_union_or();
                break;
            case condition_type::union_or:
                return make_expr_union_or();
                break;
            case condition_type::union_not:
                return make_expr_union_not();
                break;
            default:
                return nullptr;
        }
        ///   if (condition) {
        condition->set_value(value);
        ///    }
        return condition;
    }

} // namespace components::ql