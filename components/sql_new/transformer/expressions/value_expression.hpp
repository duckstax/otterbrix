#pragma once

#include "parsed_expression.hpp"
#include "sql_new/transformer/blob.hpp"
#include <utility>
#include <variant>

namespace components::sql_new::transform::expressions {
    struct value_expression : parsed_expression {
        static constexpr const ExpressionClass EXPR_CLASS = ExpressionClass::CONSTANT;
        using value_t = std::variant<int64_t, double, std::string, blob>;

        value_expression()
            : parsed_expression(ExpressionType::VALUE_CONSTANT, EXPR_CLASS) {}

        value_expression(int64_t value) // NOLINT: Allow implicit conversion from `int64`
            : parsed_expression(ExpressionType::VALUE_CONSTANT, EXPR_CLASS)
            , value(value) {}

        value_expression(double value) // NOLINT: Allow implicit conversion from `double`
            : parsed_expression(ExpressionType::VALUE_CONSTANT, EXPR_CLASS)
            , value(value) {}

        value_expression(std::string value) // NOLINT: Allow implicit conversion from `string`
            : parsed_expression(ExpressionType::VALUE_CONSTANT, EXPR_CLASS)
            , value(value) {}

        explicit value_expression(blob value)
            : parsed_expression(ExpressionType::VALUE_CONSTANT, EXPR_CLASS)
            , value(value) {}

        explicit value_expression(bool is_null)
            : parsed_expression(ExpressionType::VALUE_CONSTANT, EXPR_CLASS)
            , is_null(is_null) {}

        explicit value_expression(value_t value)
            : parsed_expression(ExpressionType::VALUE_CONSTANT, EXPR_CLASS)
            , value(std::move(value)) {}

        [[nodiscard]] std::unique_ptr<parsed_expression> make_copy() const override {
            auto copy = std::make_unique<value_expression>(value);
            copy->copy_properties(*this);
            return copy;
        }

        value_t value;
        bool is_null = false;
    };
} // namespace components::sql_new::transform::expressions
