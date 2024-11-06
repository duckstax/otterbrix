#pragma once

#include "parsed_expression.hpp"
#include <memory>
#include <string>

namespace components::sql_new::transform::expressions {
    struct parameter_expression : parsed_expression {
        static constexpr const ExpressionClass EXPR_CLASS = ExpressionClass::PARAMETER;

        parameter_expression()
            : parsed_expression(ExpressionType::VALUE_PARAMETER, EXPR_CLASS) {}

        [[nodiscard]] std::unique_ptr<parsed_expression> make_copy() const override {
            auto copy = std::make_unique<parameter_expression>();
            copy->copy_properties(*this);
            return copy;
        }

        size_t id = -1;
    };
} // namespace components::sql_new::transform::expressions