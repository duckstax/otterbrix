#pragma once

#include <string>
#include <utility>
#include <variant>
#include <vector>
#include <memory>

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

    inline bool is_union_condition(condition_type type) {
        return type == condition_type::union_and ||
               type == condition_type::union_or ||
               type == condition_type::union_not;
    }

    struct expr_t {
        using ptr = std::unique_ptr<expr_t>;

        condition_type type_;
        std::string key_;
        field_t field_;
        std::vector<ptr> sub_conditions_;

        expr_t(condition_type type, std::string key, field_t field)
            : type_(type)
            , key_(std::move(key))
            , field_(std::move(field))
            , union_(is_union_condition(type_))
        {}

        explicit expr_t(bool is_union)
            : type_(condition_type::novalid)
            , union_(is_union)
        {}

        bool is_union() const {
            return union_;
        }

        void append_sub_condition(ptr &&sub_condition) {
            sub_conditions_.push_back(std::move(sub_condition));
        }

    private:
        bool union_;
    };

    using expr_ptr = expr_t::ptr;

    template<class Value>
    expr_ptr make_expr(condition_type condition, std::string key, Value value) {
        return std::make_unique<expr_t>(condition, std::move(key), field_t(value));
    }

    template<>
    inline expr_ptr make_expr(condition_type condition, std::string key, field_t value) {
        return std::make_unique<expr_t>(condition, std::move(key), std::move(value));
    }

    inline expr_ptr make_expr() {
        return std::make_unique<expr_t>(false);
    }

    inline expr_ptr make_union_expr() {
        return std::make_unique<expr_t>(true);
    }

    inline std::string to_string(condition_type type) {
        switch (type) {
            case condition_type::eq:
                return "$eq";
            case condition_type::ne:
                return "$ne";
            case condition_type::gt:
                return "$gt";
            case condition_type::lt:
                return "$lt";
            case condition_type::gte:
                return "$gte";
            case condition_type::lte:
                return "$lte";
            case condition_type::regex:
                return "$regex";
            case condition_type::any:
                return "$any";
            case condition_type::all:
                return "$all";
            case condition_type::union_and:
                return "$and";
            case condition_type::union_or:
                return "$or";
            case condition_type::union_not:
                return "$not";
        }
        return {};
    }

    inline std::string to_string(const expr_ptr &expr) {
        if (expr->is_union()) {
            std::string result = "{\"" + to_string(expr->type_) + "\": [";
            for (std::size_t i = 0; i < expr->sub_conditions_.size(); ++ i) {
                if (i > 0) {
                    result += ", ";
                }
                result += to_string(expr->sub_conditions_.at(i));
            }
            result += "]}";
            return result;
        }
        return "{\"" + expr->key_ + "\": {\"" + to_string(expr->type_) + "\": " + expr->field_.to_string() + "}}";
    }

} // namespace components::ql