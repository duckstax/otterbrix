#include "sql_new/transformer/expressions/operator_expression.hpp"
#include "sql_new/transformer/expressions/value_expression.hpp"
#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"

using namespace components::sql_new::transform::expressions;

namespace components::sql_new::transform {
    std::unique_ptr<expressions::parsed_expression>
    transformer::transform_array_indirection(A_Indirection& indirection_node) {
        // Transform the source expression.
        std::unique_ptr<parsed_expression> result;
        result = transform_expression(indirection_node.arg);

        for (const auto& node : indirection_node.indirection->lst) {
            auto target = pg_ptr_cast<Node>(node.data);

            switch (target->type) {
                case T_A_Indices: {
                    // Index access.
                    auto indices = pg_cast<A_Indices>(*target);
                    std::vector<std::unique_ptr<parsed_expression>> children;
                    children.push_back(std::move(result));

                    // Array access.
                    // D_ASSERT(!indices.lidx && indices.uidx);
                    children.push_back(transform_expression(indices.uidx));
                    result = std::make_unique<operator_expression>(ExpressionType::ARRAY_EXTRACT, std::move(children));
                    break;
                }
                case T_String: {
                    auto value = pg_cast<Value>(*target);
                    std::vector<std::unique_ptr<parsed_expression>> children;
                    children.push_back(std::move(result));
                    children.push_back(transform_value(value));
                    result = std::make_unique<operator_expression>(ExpressionType::STRUCT_EXTRACT, std::move(children));
                    break;
                }
                case T_FuncCall: { // mdxn: func call
                                   //                    auto func = pg_cast<FuncCall>(*target);
                                   //                    auto function = TransformFuncCall(func);
                    //                    if (function->GetExpressionType() != ExpressionType::FUNCTION) {
                    //                        throw ParserException("%s.%s() call must be a function",
                    //                                              result->ToString(),
                    //                                              function->ToString());
                    //                    }
                    //                    auto& function_expr = function->Cast<FunctionExpression>();
                    //                    function_expr.children.insert(function_expr.children.begin(), std::move(result));
                    //                    result = std::move(function);
                    break;
                }
                default:
                    throw std::runtime_error("Unimplemented subscript type");
            }
        }
        return result;
    }
} // namespace components::sql_new::transform
