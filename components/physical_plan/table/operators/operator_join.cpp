#include "operator_join.hpp"

#include <services/collection/collection.hpp>
#include <vector>

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
        COMP comp{};
        auto val_left = chunk_left.data.at(str_index_map_left.at(expr->key_left().as_string())).value(row_left);
        if (expr->key_right().is_null()) {
            auto expr_val = parameters->parameters.at(expr->value());
            switch (val_left.type().to_physical_type()) {
                case components::types::physical_type::BOOL:
                    return comp(val_left.value<bool>(), expr_val.as_bool());
                case components::types::physical_type::UINT8:
                    return comp(val_left.value<uint8_t>(), expr_val.as_unsigned());
                case components::types::physical_type::INT8:
                    return comp(val_left.value<int8_t>(), expr_val.as_int());
                case components::types::physical_type::UINT16:
                    return comp(val_left.value<uint16_t>(), expr_val.as_unsigned());
                case components::types::physical_type::INT16:
                    return comp(val_left.value<int16_t>(), expr_val.as_int());
                case components::types::physical_type::UINT32:
                    return comp(val_left.value<uint32_t>(), expr_val.as_unsigned());
                case components::types::physical_type::INT32:
                    return comp(val_left.value<int32_t>(), expr_val.as_int());
                case components::types::physical_type::UINT64:
                    return comp(val_left.value<uint64_t>(), expr_val.as_unsigned());
                case components::types::physical_type::INT64:
                    return comp(val_left.value<int64_t>(), expr_val.as_int());
                // case components::types::physical_type::UINT128:
                //     return comp(val_left.value<uint128_t>(), expr_val.as_int128());
                // case components::types::physical_type::INT128:
                //     return comp(val_left.value<int128_t>(), expr_val.as_int128());
                case components::types::physical_type::FLOAT:
                    return comp(val_left.value<float>(), expr_val.as_float());
                case components::types::physical_type::DOUBLE:
                    return comp(val_left.value<double>(), expr_val.as_double());
                case components::types::physical_type::STRING:
                    return comp(val_left.value<std::string_view>(), expr_val.as_string());
                default:
                    throw std::runtime_error("invalid expression in table::operator_match");
            }
        } else {
            auto val_right = chunk_right.data.at(str_index_map_right.at(expr->key_left().as_string())).value(row_right);
            return comp(val_left, val_right);
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

    operator_join_t::operator_join_t(collection::context_collection_t* context,
                                     type join_type,
                                     const components::expressions::compare_expression_ptr& expression)
        : read_only_operator_t(context, operator_type::join)
        , join_type_(join_type)
        , expression_(std::move(expression)) {}

    bool operator_join_t::check_predicate_(components::pipeline::context_t* context,
                                           size_t row_left,
                                           size_t row_right) const {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        return check_expr_general(expression_,
                                  &context->parameters,
                                  chunk_left,
                                  chunk_right,
                                  name_index_map_left_,
                                  name_index_map_right_,
                                  row_left,
                                  row_right);
    }

    void operator_join_t::on_execute_impl(components::pipeline::context_t* context) {
        if (!left_ || !right_) {
            return;
        }
        if (left_->output() && right_->output()) {
            const auto& chunk_left = left_->output()->data_chunk();
            const auto& chunk_right = right_->output()->data_chunk();

            auto res_types = chunk_left.types();
            for (const auto& type : chunk_right.types()) {
                if (std::find(res_types.begin(), res_types.end(), type) == res_types.end()) {
                    res_types.push_back(type);
                }
            }

            output_ = base::operators::make_operator_data(left_->output()->resource(), res_types);

            if (context_) {
                // With introduction of raw_data without context, log is not guaranteed to be here
                // TODO: acquire log from different means
                trace(context_->log(), "operator_join::left_size(): {}", chunk_left.size());
                trace(context_->log(), "operator_join::right_size(): {}", chunk_right.size());
            }

            name_index_map_left_.clear();
            for (size_t i = 0; i < chunk_left.data.size(); i++) {
                name_index_map_left_.emplace(chunk_left.data[i].type().alias(), i);
            }
            name_index_map_right_.clear();
            for (size_t i = 0; i < chunk_right.data.size(); i++) {
                name_index_map_right_.emplace(chunk_right.data[i].type().alias(), i);
            }
            name_index_map_res_.clear();
            for (size_t i = 0; i < res_types.size(); i++) {
                name_index_map_res_.emplace(res_types[i].alias(), i);
            }

            switch (join_type_) {
                case type::inner:
                    inner_join_(context);
                    break;
                case type::full:
                    outer_full_join_(context);
                    break;
                case type::left:
                    outer_left_join_(context);
                    break;
                case type::right:
                    outer_right_join_(context);
                    break;
                case type::cross:
                    cross_join_(context);
                    break;
                default:
                    break;
            }

            if (context_) {
                // Same reason as above
                trace(context_->log(), "operator_join::result_size(): {}", output_->size());
            }
        }
    }

    void operator_join_t::inner_join_(components::pipeline::context_t* context) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();
        // TODO: fix edge case with same data type in both chunks
        assert(chunk_res.column_count() == chunk_left.column_count() + chunk_right.column_count());
        for (size_t i = 0; i < chunk_left.size(); i++) {
            for (size_t j = 0; j < chunk_right.size(); j++) {
                if (check_predicate_(context, i, j)) {
                    for (const auto& column : chunk_left.data) {
                        chunk_res.data[name_index_map_res_.at(column.type().alias())].push_back(column.value(i));
                    }
                    for (const auto& column : chunk_right.data) {
                        chunk_res.data[name_index_map_res_.at(column.type().alias())].push_back(column.value(i));
                    }
                }
            }
        }
    }

    void operator_join_t::outer_full_join_(components::pipeline::context_t* context) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();
        // TODO: fix edge case with same data type in both chunks
        assert(chunk_res.column_count() == chunk_left.column_count() + chunk_right.column_count());

        std::vector<bool> visited_right(right_->output()->size(), false);

        for (size_t i = 0; i < chunk_left.size(); i++) {
            bool visited_left = false;
            size_t right_index = 0;
            for (size_t j = 0; j < chunk_right.size(); j++) {
                if (check_predicate_(context, i, j)) {
                    visited_left = true;
                    visited_right[right_index] = true;
                    for (const auto& column : chunk_left.data) {
                        chunk_res.data[name_index_map_res_.at(column.type().alias())].push_back(column.value(i));
                    }
                    for (const auto& column : chunk_right.data) {
                        chunk_res.data[name_index_map_res_.at(column.type().alias())].push_back(column.value(i));
                    }
                }
                right_index++;
                if (!visited_left) {
                    for (const auto& column : chunk_left.data) {
                        chunk_res.data[name_index_map_res_.at(column.type().alias())].push_back(column.value(i));
                    }
                    for (const auto& column : chunk_right.data) {
                        chunk_res.data[name_index_map_res_.at(column.type().alias())].push_back(
                            components::types::logical_value_t{nullptr});
                    }
                }
            }
        }
        for (size_t i = 0; i < visited_right.size(); ++i) {
            if (visited_right[i]) {
                continue;
            }
            for (const auto& column : chunk_left.data) {
                chunk_res.data[name_index_map_res_.at(column.type().alias())].push_back(
                    components::types::logical_value_t{nullptr});
            }
            for (const auto& column : chunk_right.data) {
                chunk_res.data[name_index_map_res_.at(column.type().alias())].push_back(column.value(i));
            }
        }
    }

    void operator_join_t::outer_left_join_(components::pipeline::context_t* context) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();
        // TODO: fix edge case with same data type in both chunks

        for (size_t i = 0; i < chunk_left.size(); i++) {
            bool visited_left = false;
            for (size_t j = 0; j < chunk_right.size(); j++) {
                if (check_predicate_(context, i, j)) {
                    visited_left = true;
                    for (const auto& column : chunk_left.data) {
                        chunk_res.data[name_index_map_res_.at(column.type().alias())].push_back(column.value(i));
                    }
                    for (const auto& column : chunk_right.data) {
                        chunk_res.data[name_index_map_res_.at(column.type().alias())].push_back(column.value(i));
                    }
                }
                if (!visited_left) {
                    for (const auto& column : chunk_left.data) {
                        chunk_res.data[name_index_map_res_.at(column.type().alias())].push_back(column.value(i));
                    }
                    for (const auto& column : chunk_right.data) {
                        chunk_res.data[name_index_map_res_.at(column.type().alias())].push_back(
                            components::types::logical_value_t{nullptr});
                    }
                }
            }
        }
    }

    void operator_join_t::outer_right_join_(components::pipeline::context_t* context) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();
        // TODO: fix edge case with same data type in both chunks

        for (size_t i = 0; i < chunk_right.size(); i++) {
            bool visited_right = false;
            for (size_t j = 0; j < chunk_left.size(); j++) {
                if (check_predicate_(context, i, j)) {
                    visited_right = true;
                    for (const auto& column : chunk_left.data) {
                        chunk_res.data[name_index_map_res_.at(column.type().alias())].push_back(column.value(i));
                    }
                    for (const auto& column : chunk_right.data) {
                        chunk_res.data[name_index_map_res_.at(column.type().alias())].push_back(column.value(i));
                    }
                }
                if (!visited_right) {
                    for (const auto& column : chunk_left.data) {
                        chunk_res.data[name_index_map_res_.at(column.type().alias())].push_back(
                            components::types::logical_value_t{nullptr});
                    }
                    for (const auto& column : chunk_right.data) {
                        chunk_res.data[name_index_map_res_.at(column.type().alias())].push_back(column.value(i));
                    }
                }
            }
        }
    }

    void operator_join_t::cross_join_(components::pipeline::context_t* context) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();
        // TODO: fix edge case with same data type in both chunks
        assert(chunk_res.column_count() == chunk_left.column_count() + chunk_right.column_count());
        for (size_t i = 0; i < chunk_left.size(); i++) {
            for (size_t j = 0; j < chunk_right.size(); j++) {
                for (const auto& column : chunk_left.data) {
                    chunk_res.data[name_index_map_res_.at(column.type().alias())].push_back(column.value(i));
                }
                for (const auto& column : chunk_right.data) {
                    chunk_res.data[name_index_map_res_.at(column.type().alias())].push_back(column.value(i));
                }
            }
        }
    }

} // namespace services::table::operators
