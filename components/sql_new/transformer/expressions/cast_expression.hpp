#pragma once

#include "parsed_expression.hpp"
#include <memory>

namespace components::sql_new::transform {
    enum struct logical_type : uint8_t
    {
        BOOLEAN
    };
}

namespace components::sql_new::transform::expressions {
    struct cast_expression : parsed_expression {
        static constexpr const ExpressionClass EXPR_CLASS = ExpressionClass::CAST;

        cast_expression(logical_type target, std::unique_ptr<parsed_expression> child, bool is_try_cast = false)
            : parsed_expression(ExpressionType::OPERATOR_CAST, EXPR_CLASS)
            , cast_type(target)
            , child(std::move(child))
            , is_try_cast(is_try_cast) {}

        [[nodiscard]] std::unique_ptr<parsed_expression> make_copy() const override {
            auto copy = std::make_unique<cast_expression>(cast_type, child->make_copy(), is_try_cast);
            copy->copy_properties(*this);
            return copy;
        }

        std::unique_ptr<parsed_expression> child;
        logical_type cast_type;
        bool is_try_cast;
    };
} // namespace components::sql_new::transform::expressions
