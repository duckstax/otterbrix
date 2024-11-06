#pragma once

#include "parsed_expression.hpp"
#include <memory>
#include <string>

namespace components::sql_new::transform::expressions {
    struct collate_expression : parsed_expression {
        static constexpr const ExpressionClass EXPR_CLASS = ExpressionClass::COLLATE;

        collate_expression(std::string collation, std::unique_ptr<parsed_expression> child)
            : parsed_expression(ExpressionType::COLLATE, EXPR_CLASS)
            , child(std::move(child))
            , collation(std::move(collation)) {}

        [[nodiscard]] std::unique_ptr<parsed_expression> make_copy() const override {
            auto copy = std::make_unique<collate_expression>(collation, child->make_copy());
            copy->copy_properties(*this);
            return copy;
        }

        std::unique_ptr<parsed_expression> child;
        std::string collation;
    };
} // namespace components::sql_new::transform::expressions