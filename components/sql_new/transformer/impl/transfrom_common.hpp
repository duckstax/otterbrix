#pragma once
#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"
#include <document/value.hpp>
#include <expressions/compare_expression.hpp>
#include <expressions/scalar_expression.hpp>

namespace components::sql_new::transform::impl {

    std::pair<document::value_t, std::string> get_value(Node* node, document::impl::base_document* tape);

    components::expressions::compare_expression_ptr transform_a_expr(std::pmr::memory_resource* resource, A_Expr* node);
} // namespace components::sql_new::transform::impl
