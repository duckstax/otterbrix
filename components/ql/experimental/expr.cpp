#include "expr.hpp"
#include <sstream>

namespace components::ql::experimental {

    bool is_union_condition(condition_type type) {
        return type == condition_type::union_and ||
               type == condition_type::union_or ||
               type == condition_type::union_not;
    }

    std::string to_string(const expr_ptr& expr) {
        std::stringstream stream;
        stream << expr;
        return stream.str();
    }

    expr_t::expr_t(condition_type type, std::string key, core::parameter_id_t value)
        : type_(type)
        , key_(std::move(key))
        , value_(value)
        , union_(is_union_condition(type_)) {}

    expr_t::expr_t(bool is_union)
        : type_(condition_type::novalid)
        , value_(0)
        , union_(is_union) {}

    bool expr_t::is_union() const {
        return union_;
    }

    void expr_t::append_sub_condition(expr_t::ptr sub_condition) {
        sub_conditions_.push_back(std::move(sub_condition));
    }

    expr_ptr make_expr(condition_type condition, const std::string& key, core::parameter_id_t id) {
        return std::make_unique<expr_t>(condition, key, id);
    }

    expr_ptr make_expr() {
        return std::make_unique<expr_t>(false);
    }

    expr_ptr make_union_expr() {
        return std::make_unique<expr_t>(true);
    }

} // namespace components::ql::experiment