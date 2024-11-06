#pragma once

#include "parsed_expression.hpp"
#include <memory>
#include <vector>

namespace components::sql_new::transform::expressions {
    struct operator_expression : parsed_expression {
        static constexpr const ExpressionClass EXPR_CLASS = ExpressionClass::OPERATOR;

        operator_expression(ExpressionType type, std::vector<std::unique_ptr<parsed_expression>> children)
            : parsed_expression(type, EXPR_CLASS)
            , children(std::move(children)) {}

        explicit operator_expression(ExpressionType type,
                                     std::unique_ptr<parsed_expression> left = nullptr,
                                     std::unique_ptr<parsed_expression> right = nullptr)
            : parsed_expression(type, EXPR_CLASS) {
            if (left) {
                children.push_back(std::move(left));
            }

            if (right) {
                children.push_back(std::move(right));
            }
        }

        [[nodiscard]] std::unique_ptr<parsed_expression> make_copy() const override {
            auto copy = std::make_unique<operator_expression>(type);
            copy->copy_properties(*this);
            for (auto& it : children) {
                copy->children.push_back(it->make_copy());
            }
            return copy;
        }

        std::vector<std::unique_ptr<parsed_expression>> children;
    };
} // namespace components::sql_new::transform::expressions