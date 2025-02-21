#pragma once

#include "parsed_expression.hpp"
#include <vector>

namespace components::sql_new::transform::expressions {
    struct columnref_expression : parsed_expression {
        static constexpr const ExpressionClass EXPR_CLASS = ExpressionClass::COLUMN_REF;

        columnref_expression()
            : parsed_expression(ExpressionType::COLUMN_REF, EXPR_CLASS) {}

        explicit columnref_expression(bool is_star)
            : parsed_expression(ExpressionType::COLUMN_REF, EXPR_CLASS)
            , is_star(is_star) {}

        //        columnref_expression(std::string column_name, std::string table_name); // ????

        explicit columnref_expression(std::vector<std::string> column_names)
            : parsed_expression(ExpressionType::COLUMN_REF, EXPR_CLASS)
            , column_names(std::move(column_names)) {}

        [[nodiscard]] std::unique_ptr<parsed_expression> make_copy() const override {
            auto copy = std::make_unique<columnref_expression>(column_names);
            copy->copy_properties(*this);
            return std::move(copy);
        }

        std::vector<std::string> column_names;
        bool is_star = false;
    };
} // namespace components::sql_new::transform::expressions
