#pragma once

#include "parsed_expression.hpp"
#include <memory>
#include <vector>

namespace components::sql_new::transform::expressions {
    struct conjunction_expression : parsed_expression {
        static constexpr const ExpressionClass EXPR_CLASS = ExpressionClass::CONJUNCTION;

        explicit conjunction_expression(ExpressionType type)
            : parsed_expression(type, EXPR_CLASS) {}

        conjunction_expression(ExpressionType type, std::vector<std::unique_ptr<parsed_expression>> children)
            : parsed_expression(type, EXPR_CLASS) {
            for (auto& child : children) {
                append_expression(std::move(child));
            }
        }

        conjunction_expression(ExpressionType type,
                               std::unique_ptr<parsed_expression> left,
                               std::unique_ptr<parsed_expression> right)
            : parsed_expression(type, EXPR_CLASS) {
            append_expression(std::move(left));
            append_expression(std::move(right));
        }

        void append_expression(std::unique_ptr<parsed_expression> expr) {
            if (expr->type == type) {
                // expr is a conjunction of the same type: merge the expression lists together
                auto& other = expr->cast<conjunction_expression>();
                for (auto& child : other.children) {
                    children.push_back(std::move(child));
                }
            } else {
                children.push_back(std::move(expr));
            }
        }

        [[nodiscard]] std::unique_ptr<parsed_expression> make_copy() const override {
            std::vector<std::unique_ptr<parsed_expression>> copy_children;
            copy_children.reserve(children.size());
            for (auto& expr : children) {
                copy_children.push_back(expr->make_copy());
            }

            auto copy = std::make_unique<conjunction_expression>(type, std::move(copy_children));
            copy->copy_properties(*this);
            return std::move(copy);
        }

        std::vector<std::unique_ptr<parsed_expression>> children;
    };
} // namespace components::sql_new::transform::expressions