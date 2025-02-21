#pragma once

#include "base_expression.hpp"
#include <memory>

namespace components::sql_new::transform::expressions {
    struct parsed_expression : base_expression {
        parsed_expression(ExpressionType type, ExpressionClass expression_class)
            : base_expression(type, expression_class) {}

        [[nodiscard]] virtual std::unique_ptr<parsed_expression> make_copy() const = 0;

        [[nodiscard]] bool is_same(const base_expression& other) const override;

    protected:
        void copy_properties(const parsed_expression& other);
    };
} // namespace components::sql_new::transform::expressions