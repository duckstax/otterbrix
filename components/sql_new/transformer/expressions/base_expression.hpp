#pragma once

#include "expression_enums.hpp"
#include <stdexcept>
#include <string>

namespace components::sql_new::transform::expressions {
    struct base_expression {
        base_expression(ExpressionType type, ExpressionClass expression_class)
            : type(type)
            , expression_class(expression_class) {}

        virtual ~base_expression() = default; // {} orig

        [[nodiscard]] virtual bool is_same(const base_expression& other) const {
            return expression_class == other.expression_class && type == other.type;
        }

        bool operator==(const base_expression& rhs) const { return is_same(rhs); }

        template<class T>
        T& cast() {
            if (expression_class != T::EXPR_CLASS) {
                throw std::runtime_error("Failed to cast expression to type - expression type mismatch");
            }
            return reinterpret_cast<T&>(*this);
        }

        template<class T>
        const T& cast() const {
            if (expression_class != T::EXPR_CLASS) {
                throw std::runtime_error("Failed to cast expression to type - expression type mismatch");
            }
            return reinterpret_cast<const T&>(*this);
        }

        ExpressionType type;
        ExpressionClass expression_class;
        std::string alias;
        int location = -1;
    };
} // namespace components::sql_new::transform::expressions