#pragma once

#include "parsed_expression.hpp"
#include <memory>

namespace components::sql_new::transform::expressions {
    struct default_expression : parsed_expression {
        static constexpr const ExpressionClass EXPR_CLASS = ExpressionClass::DEFAULT;

        default_expression()
            : parsed_expression(ExpressionType::VALUE_DEFAULT, EXPR_CLASS) {}

        [[nodiscard]] std::unique_ptr<parsed_expression> make_copy() const override {
            auto copy = std::make_unique<default_expression>();
            copy->copy_properties(*this);
            return copy;
        }
    };
} // namespace components::sql_new::transform::expressions
