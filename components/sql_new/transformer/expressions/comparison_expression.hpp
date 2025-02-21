#pragma once

#include "parsed_expression.hpp"
#include <memory>
#include <vector>

namespace components::sql_new::transform::expressions {
    struct comparison_expression : parsed_expression {
        static constexpr const ExpressionClass EXPR_CLASS = ExpressionClass::COMPARISON;

        explicit comparison_expression(ExpressionType type,
                                       std::unique_ptr<parsed_expression> left,
                                       std::unique_ptr<parsed_expression> right)
            : parsed_expression(type, EXPR_CLASS)
            , left(std::move(left))
            , right(std::move(right)) {}

        [[nodiscard]] std::unique_ptr<parsed_expression> make_copy() const override {
            auto copy = std::make_unique<comparison_expression>(type, left->make_copy(), right->make_copy());
            copy->copy_properties(*this);
            return copy;
        }

        std::unique_ptr<parsed_expression> left;
        std::unique_ptr<parsed_expression> right;
    };
} // namespace components::sql_new::transform::expressions