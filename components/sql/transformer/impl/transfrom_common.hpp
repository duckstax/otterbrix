#pragma once
#include "sql/transformer/transformer.hpp"
#include "sql/transformer/utils.hpp"
#include <document/value.hpp>
#include <expressions/compare_expression.hpp>
#include <expressions/scalar_expression.hpp>

namespace components::sql::transform::impl {

    std::pair<document::value_t, std::string> get_value(Node* node, document::impl::base_document* tape);

    expressions::compare_expression_ptr transform_a_expr(logical_plan::parameter_node_t* statement, A_Expr* node);
    components::expressions::compare_expression_ptr transform_a_indirection(logical_plan::parameter_node_t* statement,
                                                                            A_Indirection* node);

    logical_plan::node_ptr transform_function(RangeFunction& node, logical_plan::parameter_node_t* statement);
} // namespace components::sql::transform::impl
