#pragma once

#include "parsed_expression.hpp"
#include <memory>
#include <vector>

namespace components::sql_new::transform::expressions {
    struct case_check {
        std::unique_ptr<parsed_expression> when_expr;
        std::unique_ptr<parsed_expression> then_expr;
    };

    struct case_expression : parsed_expression {
        static constexpr const ExpressionClass EXPR_CLASS = ExpressionClass::CASE;

    public:
        case_expression()
            : parsed_expression(ExpressionType::CASE_EXPR, EXPR_CLASS) {}

        [[nodiscard]] std::unique_ptr<parsed_expression> make_copy() const override {
            auto copy = std::make_unique<case_expression>();
            copy->copy_properties(*this);
            for (auto& check : case_checks) {
                case_check new_check;
                new_check.when_expr = check.when_expr->make_copy();
                new_check.then_expr = check.then_expr->make_copy();
                copy->case_checks.push_back(std::move(new_check));
            }

            copy->else_expr = else_expr->make_copy();
            return copy;
        }

        std::vector<case_check> case_checks;
        std::unique_ptr<parsed_expression> else_expr;
    };
} // namespace components::sql_new::transform::expressions