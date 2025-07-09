#include "check_expr.hpp"

namespace services::table::operators {

    template<typename COMP>
    bool check_expr(const components::expressions::compare_expression_ptr& expr,
                    const components::logical_plan::storage_parameters* parameters,
                    const components::vector::data_chunk_t& chunk_left,
                    const components::vector::data_chunk_t& chunk_right,
                    const std::unordered_map<std::string, size_t>& str_index_map_left,
                    const std::unordered_map<std::string, size_t>& str_index_map_right,
                    size_t row_left,
                    size_t row_right) {
        auto val_left = chunk_left.data.at(str_index_map_left.at(expr->key_left().as_string())).value(row_left);
        auto val_right =
            expr->key_right().is_null()
                ? parameters->parameters.at(expr->value()).as_logical_value()
                : chunk_right.data.at(str_index_map_right.at(expr->key_right().as_string())).value(row_right);
        COMP comp{};
        switch (val_left.type().to_physical_type()) {
            case components::types::physical_type::BOOL:
                return comp(val_left.value<bool>(), val_right.value<bool>());
            case components::types::physical_type::UINT8:
                return comp(val_left.value<uint8_t>(), val_right.value<uint8_t>());
            case components::types::physical_type::INT8:
                return comp(val_left.value<int8_t>(), val_right.value<int8_t>());
            case components::types::physical_type::UINT16:
                return comp(val_left.value<uint16_t>(), val_right.value<uint16_t>());
            case components::types::physical_type::INT16:
                return comp(val_left.value<int16_t>(), val_right.value<int16_t>());
            case components::types::physical_type::UINT32:
                return comp(val_left.value<uint32_t>(), val_right.value<uint32_t>());
            case components::types::physical_type::INT32:
                return comp(val_left.value<int32_t>(), val_right.value<int32_t>());
            case components::types::physical_type::UINT64:
                return comp(val_left.value<uint64_t>(), val_right.value<uint64_t>());
            case components::types::physical_type::INT64:
                return comp(val_left.value<int64_t>(), val_right.value<int64_t>());
                // case components::types::physical_type::UINT128:
                //     return comp(val_left.value<uint128_t>(), val_right.value<uint128_t>());
                // case components::types::physical_type::INT128:
                //     return comp(val_left.value<int128_t>(), val_right.value<int128_t>());
            case components::types::physical_type::FLOAT:
                return comp(val_left.value<float>(), val_right.value<float>());
            case components::types::physical_type::DOUBLE:
                return comp(val_left.value<double>(), val_right.value<double>());
            case components::types::physical_type::STRING:
                return comp(val_left.value<std::string_view>(), val_right.value<std::string_view>());
            default:
                throw std::runtime_error("invalid expression in table::operator_match");
        }
    }

    template<typename COMP>
    bool check_expr(const components::expressions::compare_expression_ptr& expr,
                    const components::logical_plan::storage_parameters* parameters,
                    const components::vector::data_chunk_t& chunk,
                    const std::unordered_map<std::string, size_t>& str_index_map,
                    size_t row) {
        auto val = chunk.data.at(str_index_map.at(expr->key_left().as_string())).value(row);
        auto expr_val = parameters->parameters.at(expr->value());
        COMP comp{};
        switch (val.type().to_physical_type()) {
            case components::types::physical_type::BOOL:
                return comp(val.value<bool>(), expr_val.as_bool());
            case components::types::physical_type::UINT8:
                return comp(val.value<uint8_t>(), expr_val.as_unsigned());
            case components::types::physical_type::INT8:
                return comp(val.value<int8_t>(), expr_val.as_int());
            case components::types::physical_type::UINT16:
                return comp(val.value<uint16_t>(), expr_val.as_unsigned());
            case components::types::physical_type::INT16:
                return comp(val.value<int16_t>(), expr_val.as_int());
            case components::types::physical_type::UINT32:
                return comp(val.value<uint32_t>(), expr_val.as_unsigned());
            case components::types::physical_type::INT32:
                return comp(val.value<int32_t>(), expr_val.as_int());
            case components::types::physical_type::UINT64:
                return comp(val.value<uint64_t>(), expr_val.as_unsigned());
            case components::types::physical_type::INT64:
                return comp(val.value<int64_t>(), expr_val.as_int());
                // case components::types::physical_type::UINT128:
                //     return comp(val.value<uint128_t>(), expr_val.as_int128());
                // case components::types::physical_type::INT128:
                //     return comp(val.value<int128_t>(), expr_val.as_int128());
            case components::types::physical_type::FLOAT:
                return comp(val.value<float>(), expr_val.as_float());
            case components::types::physical_type::DOUBLE:
                return comp(val.value<double>(), expr_val.as_double());
            case components::types::physical_type::STRING:
                return comp(val.value<std::string_view>(), expr_val.as_string());
            default:
                throw std::runtime_error("invalid expression in table::operator_match");
        }
    }

    bool check_expr_general(const components::expressions::compare_expression_ptr& expr,
                            const components::logical_plan::storage_parameters* parameters,
                            const components::vector::data_chunk_t& chunk_left,
                            const components::vector::data_chunk_t& chunk_right,
                            const std::unordered_map<std::string, size_t>& str_index_map_left,
                            const std::unordered_map<std::string, size_t>& str_index_map_right,
                            size_t row_left,
                            size_t row_right) {
        switch (expr->type()) {
            case components::expressions::compare_type::union_and: {
                for (const auto& child_expr : expr->children()) {
                    if (!check_expr_general(
                            reinterpret_cast<const components::expressions::compare_expression_ptr&>(child_expr),
                            parameters,
                            chunk_left,
                            chunk_right,
                            str_index_map_left,
                            str_index_map_right,
                            row_left,
                            row_right)) {
                        return false;
                    }
                }
                return true;
            }
            case components::expressions::compare_type::union_or: {
                for (const auto& child_expr : expr->children()) {
                    if (check_expr_general(
                            reinterpret_cast<const components::expressions::compare_expression_ptr&>(child_expr),
                            parameters,
                            chunk_left,
                            chunk_right,
                            str_index_map_left,
                            str_index_map_right,
                            row_left,
                            row_right)) {
                        return true;
                    }
                }
                return false;
            }
            case components::expressions::compare_type::union_not:
                return !check_expr_general(
                    reinterpret_cast<const components::expressions::compare_expression_ptr&>(expr->children().front()),
                    parameters,
                    chunk_left,
                    chunk_right,
                    str_index_map_left,
                    str_index_map_right,
                    row_left,
                    row_right);
            case components::expressions::compare_type::eq:
                return check_expr<std::equal_to<>>(expr,
                                                   parameters,
                                                   chunk_left,
                                                   chunk_right,
                                                   str_index_map_left,
                                                   str_index_map_right,
                                                   row_left,
                                                   row_right);
            case components::expressions::compare_type::ne:
                return check_expr<std::not_equal_to<>>(expr,
                                                       parameters,
                                                       chunk_left,
                                                       chunk_right,
                                                       str_index_map_left,
                                                       str_index_map_right,
                                                       row_left,
                                                       row_right);
            case components::expressions::compare_type::gt:
                return check_expr<std::greater<>>(expr,
                                                  parameters,
                                                  chunk_left,
                                                  chunk_right,
                                                  str_index_map_left,
                                                  str_index_map_right,
                                                  row_left,
                                                  row_right);
            case components::expressions::compare_type::lt:
                return check_expr<std::less<>>(expr,
                                               parameters,
                                               chunk_left,
                                               chunk_right,
                                               str_index_map_left,
                                               str_index_map_right,
                                               row_left,
                                               row_right);
            case components::expressions::compare_type::gte:
                return check_expr<std::greater_equal<>>(expr,
                                                        parameters,
                                                        chunk_left,
                                                        chunk_right,
                                                        str_index_map_left,
                                                        str_index_map_right,
                                                        row_left,
                                                        row_right);
            case components::expressions::compare_type::lte:
                return check_expr<std::less_equal<>>(expr,
                                                     parameters,
                                                     chunk_left,
                                                     chunk_right,
                                                     str_index_map_left,
                                                     str_index_map_right,
                                                     row_left,
                                                     row_right);
            case components::expressions::compare_type::any:
            case components::expressions::compare_type::all:
            case components::expressions::compare_type::all_true:
                return false;
            case components::expressions::compare_type::all_false:
                return false;
            default:
                throw std::runtime_error("invalid expression in table::operator_match");
                return false;
        }
    }

    bool check_expr_general(const components::expressions::compare_expression_ptr& expr,
                            const components::logical_plan::storage_parameters* parameters,
                            const components::vector::data_chunk_t& chunk,
                            const std::unordered_map<std::string, size_t>& str_index_map,
                            size_t row) {
        switch (expr->type()) {
            case components::expressions::compare_type::union_and: {
                for (const auto& child_expr : expr->children()) {
                    if (!check_expr_general(
                            reinterpret_cast<const components::expressions::compare_expression_ptr&>(child_expr),
                            parameters,
                            chunk,
                            str_index_map,
                            row)) {
                        return false;
                    }
                }
                return true;
            }
            case components::expressions::compare_type::union_or: {
                for (const auto& child_expr : expr->children()) {
                    if (check_expr_general(
                            reinterpret_cast<const components::expressions::compare_expression_ptr&>(child_expr),
                            parameters,
                            chunk,
                            str_index_map,
                            row)) {
                        return true;
                    }
                }
                return false;
            }
            case components::expressions::compare_type::union_not:
                return !check_expr_general(
                    reinterpret_cast<const components::expressions::compare_expression_ptr&>(expr->children().front()),
                    parameters,
                    chunk,
                    str_index_map,
                    row);
            case components::expressions::compare_type::eq:
                return check_expr<std::equal_to<>>(expr, parameters, chunk, str_index_map, row);
            case components::expressions::compare_type::ne:
                return check_expr<std::not_equal_to<>>(expr, parameters, chunk, str_index_map, row);
            case components::expressions::compare_type::gt:
                return check_expr<std::greater<>>(expr, parameters, chunk, str_index_map, row);
            case components::expressions::compare_type::lt:
                return check_expr<std::less<>>(expr, parameters, chunk, str_index_map, row);
            case components::expressions::compare_type::gte:
                return check_expr<std::greater_equal<>>(expr, parameters, chunk, str_index_map, row);
            case components::expressions::compare_type::lte:
                return check_expr<std::less_equal<>>(expr, parameters, chunk, str_index_map, row);
            case components::expressions::compare_type::any:
            case components::expressions::compare_type::all:
            case components::expressions::compare_type::all_true:
                return false;
            case components::expressions::compare_type::all_false:
                return false;
            default:
                throw std::runtime_error("invalid expression in table::operator_match");
                return false;
        }
    }

} // namespace services::table::operators