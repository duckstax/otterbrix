#include "operator_match.hpp"

namespace services::table::operators {

    operator_match_t::operator_match_t(collection::context_collection_t* context,
                                       const components::expressions::compare_expression_ptr& expression,
                                       components::logical_plan::limit_t limit)
        : read_only_operator_t(context, operator_type::match)
        , expression_(std::move(expression))
        , limit_(limit) {}

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

    void operator_match_t::on_execute_impl(components::pipeline::context_t* pipeline_context) {
        int count = 0;
        if (!limit_.check(count)) {
            return; //limit = 0
        }
        if (!left_) {
            return;
        }
        if (left_->output()) {
            const auto& chunk = std::get<components::vector::data_chunk_t>(left_->output()->data());
            auto types = chunk.types();
            std::unordered_map<std::string, size_t> name_index_map;
            for (size_t i = 0; i < types.size(); i++) {
                name_index_map.emplace(types[i].alias(), i);
            }
            output_ = base::operators::make_operator_data(left_->output()->resource(), types);
            auto& out_chunk = std::get<components::vector::data_chunk_t>(output_->data());
            for (size_t i = 0; i < chunk.size(); i++) {
                if (check_expr_general(expression_, &pipeline_context->parameters, chunk, name_index_map, i)) {
                    for (size_t j = 0; j < chunk.column_count(); j++) {
                        out_chunk.data[j].push_back(chunk.data[j].value(i));
                        ++count;
                        if (!limit_.check(count)) {
                            return;
                        }
                    }
                }
            }
        }
    }

} // namespace services::table::operators
