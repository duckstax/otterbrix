#pragma once

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::table::operators {

    bool check_expr_general(const expressions::compare_expression_ptr& expr,
                            const logical_plan::storage_parameters* parameters,
                            const vector::data_chunk_t& chunk_left,
                            const vector::data_chunk_t& chunk_right,
                            const std::unordered_map<std::string, size_t>& str_index_map_left,
                            const std::unordered_map<std::string, size_t>& str_index_map_right,
                            size_t row_left,
                            size_t row_right);

    bool check_expr_general(const expressions::compare_expression_ptr& expr,
                            const logical_plan::storage_parameters* parameters,
                            const vector::data_chunk_t& chunk,
                            const std::unordered_map<std::string, size_t>& str_index_map,
                            size_t row);

} // namespace components::table::operators