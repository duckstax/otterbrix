#pragma once

#include "parsed_expression.hpp"
#include <memory>

namespace components::sql_new::transform::expressions {
    struct star_expression : parsed_expression {
        static constexpr const ExpressionClass EXPR_CLASS = ExpressionClass::STAR;

        star_expression()
            : parsed_expression(ExpressionType::STAR, EXPR_CLASS) {}

        [[nodiscard]] std::unique_ptr<parsed_expression> make_copy() const override {
            auto copy = std::make_unique<star_expression>();
            copy->copy_properties(*this);
            return copy;
        }
    };
} // namespace components::sql_new::transform::expressions
