#pragma once
#include <components/document/value.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

namespace components::sql::transform::impl {

    std::pair<document::value_t, std::string> get_value(Node* node, document::impl::base_document* tape);

    expressions::compare_expression_ptr transform_a_expr(logical_plan::parameter_node_t* statement, A_Expr* node);
    components::expressions::compare_expression_ptr transform_a_indirection(logical_plan::parameter_node_t* statement,
                                                                            A_Indirection* node);

    logical_plan::node_ptr transform_function(RangeFunction& node, logical_plan::parameter_node_t* statement);

    logical_plan::node_ptr transform_function(FuncCall& node, logical_plan::parameter_node_t* statement);
} // namespace components::sql::transform::impl
