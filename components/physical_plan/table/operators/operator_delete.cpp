#include "operator_delete.hpp"
#include "check_expr.hpp"
#include <services/collection/collection.hpp>

namespace components::table::operators {

    operator_delete::operator_delete(services::collection::context_collection_t* context,
                                     expressions::compare_expression_ptr expr)
        : read_write_operator_t(context, operator_type::remove)
        , compare_expression_(std::move(expr)) {}

    void operator_delete::on_execute_impl(pipeline::context_t* pipeline_context) {
        // TODO: worth to create separate update_join operator or mutable_join with callback
        if (left_ && left_->output() && right_ && right_->output()) {
            modified_ = base::operators::make_operator_write_data<size_t>(context_->resource());
            auto& chunk_left = left_->output()->data_chunk();
            auto& chunk_right = right_->output()->data_chunk();
            auto types_left = chunk_left.types();
            auto types_right = chunk_left.types();
            std::unordered_map<std::string, size_t> name_index_map_left;
            for (size_t i = 0; i < types_left.size(); i++) {
                name_index_map_left.emplace(types_left[i].alias(), i);
            }
            std::unordered_map<std::string, size_t> name_index_map_right;
            for (size_t i = 0; i < types_right.size(); i++) {
                name_index_map_right.emplace(types_right[i].alias(), i);
            }

            vector::vector_t ids(left_->output()->resource(),
                                 logical_type::BIGINT,
                                 chunk_left.size() * chunk_right.size());

            size_t index = 0;
            for (size_t i = 0; i < chunk_left.size(); i++) {
                for (size_t j = 0; j < chunk_right.size(); j++) {
                    if (check_expr_general(compare_expression_,
                                           &pipeline_context->parameters,
                                           chunk_left,
                                           chunk_right,
                                           name_index_map_left,
                                           name_index_map_right,
                                           i,
                                           j)) {
                        ids.set_value(index++, types::logical_value_t{static_cast<int64_t>(i)});
                    }
                }
            }
            ids.resize(chunk_left.size() * chunk_right.size(), index);
            auto state = context_->table_storage().table().initialize_delete({});
            context_->table_storage().table().delete_rows(*state, ids, ids.size());
            for (size_t i = 0; i < ids.size(); i++) {
                size_t id = *ids.data<int64_t>();
                modified_->append(id);
                context_->index_engine()->delete_row(chunk_left, id, pipeline_context);
            }
        } else if (left_ && left_->output()) {
            modified_ = base::operators::make_operator_write_data<size_t>(context_->resource());
            auto& chunk = left_->output()->data_chunk();
            auto types = chunk.types();
            std::unordered_map<std::string, size_t> name_index_map;
            for (size_t i = 0; i < types.size(); i++) {
                name_index_map.emplace(types[i].alias(), i);
            }

            vector::vector_t ids(left_->output()->resource(), logical_type::BIGINT, chunk.size());

            size_t index = 0;
            for (size_t i = 0; i < chunk.size(); i++) {
                if (check_expr_general(compare_expression_, &pipeline_context->parameters, chunk, name_index_map, i)) {
                    if (chunk.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
                        ids.set_value(
                            index++,
                            types::logical_value_t{static_cast<int64_t>(chunk.data.front().indexing().get_index(i))});
                    } else {
                        ids.set_value(index++, chunk.row_ids.value(i));
                    }
                }
            }
            ids.resize(chunk.size(), index);
            auto state = context_->table_storage().table().initialize_delete({});
            context_->table_storage().table().delete_rows(*state, ids, index);
            for (size_t i = 0; i < index; i++) {
                size_t id = ids.data<int64_t>()[i];
                modified_->append(id);
                context_->index_engine()->delete_row(chunk, i, pipeline_context);
            }
        }
    }

} // namespace components::table::operators
