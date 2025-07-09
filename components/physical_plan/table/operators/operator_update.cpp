#include "operator_update.hpp"
#include "check_expr.hpp"

#include <services/collection/collection.hpp>

namespace services::table::operators {

    operator_update::operator_update(collection::context_collection_t* context,
                                     std::pmr::vector<components::expressions::update_expr_ptr> updates,
                                     bool upsert,
                                     components::expressions::compare_expression_ptr comp_expr)
        : read_write_operator_t(context, operator_type::update)
        , updates_(std::move(updates))
        , comp_expr_(std::move(comp_expr))
        , upsert_(upsert) {}

    void operator_update::on_execute_impl(components::pipeline::context_t* pipeline_context) {
        // TODO: worth to create separate update_join operator or mutable_join with callback
        // TODO: update indexes
        if (left_ && left_->output() && right_ && right_->output()) {
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

            if (left_->output()->data_chunk().size() == 0 && right_->output()->data_chunk().size() == 0) {
                if (upsert_) {
                    output_ = base::operators::make_operator_data(context_->resource(), types_left);
                    for (const auto& expr : updates_) {
                        expr->execute(chunk_left, chunk_right, 0, 0, &pipeline_context->parameters);
                    }
                    modified_ = base::operators::make_operator_write_data<size_t>(context_->resource());
                    components::table::table_append_state state(context_->resource());
                    context_->table_storage().table().initialize_append(state);
                    for (size_t id = 0; id < output_->data_chunk().size(); id++) {
                        modified_->append(id + state.row_start);
                    }
                    context_->table_storage().table().append(output_->data_chunk(), state);
                }
            } else {
                modified_ = base::operators::make_operator_write_data<size_t>(context_->resource());
                no_modified_ = base::operators::make_operator_write_data<size_t>(context_->resource());
                output_ = base::operators::make_operator_data(left_->output()->resource(), types_left);
                auto state = context_->table_storage().table().initialize_update({});
                components::vector::vector_t row_ids(context_->resource(), logical_type::BIGINT);
                auto& out_chunk = output_->data_chunk();
                for (size_t i = 0; i < chunk_left.size(); i++) {
                    for (size_t j = 0; j < chunk_right.size(); j++) {
                        if (check_expr_general(comp_expr_,
                                               &pipeline_context->parameters,
                                               chunk_left,
                                               chunk_right,
                                               name_index_map_left,
                                               name_index_map_right,
                                               i,
                                               j)) {
                            row_ids.push_back(components::types::logical_value_t{static_cast<int64_t>(i)});
                            bool modified = false;
                            for (const auto& expr : updates_) {
                                modified |= expr->execute(chunk_left, chunk_right, i, j, &pipeline_context->parameters);
                            }
                            if (modified) {
                                modified_->append(i);
                            } else {
                                no_modified_->append(i);
                            }
                            for (size_t k = 0; k < chunk_left.column_count(); k++) {
                                out_chunk.data[k].push_back(chunk_left.data[k].value(i));
                            }
                        }
                    }
                }
                context_->table_storage().table().update(*state, row_ids, chunk_left);
            }
        } else if (left_ && left_->output()) {
            if (left_->output()->size() == 0) {
                if (upsert_) {
                    output_ = base::operators::make_operator_data(context_->resource(),
                                                                  left_->output()->data_chunk().types());

                    components::table::table_append_state state(context_->resource());
                    context_->table_storage().table().initialize_append(state);
                    context_->table_storage().table().append(output_->data_chunk(), state);
                }
            } else {
                auto& chunk = left_->output()->data_chunk();
                auto types = chunk.types();
                std::unordered_map<std::string, size_t> name_index_map;
                for (size_t i = 0; i < types.size(); i++) {
                    name_index_map.emplace(types[i].alias(), i);
                }
                output_ = base::operators::make_operator_data(left_->output()->resource(), types);
                auto& out_chunk = output_->data_chunk();
                modified_ = base::operators::make_operator_write_data<size_t>(context_->resource());
                no_modified_ = base::operators::make_operator_write_data<size_t>(context_->resource());
                auto state = context_->table_storage().table().initialize_update({});
                components::vector::vector_t row_ids(context_->resource(), logical_type::BIGINT);
                for (size_t i = 0; i < chunk.size(); i++) {
                    if (check_expr_general(comp_expr_, &pipeline_context->parameters, chunk, name_index_map, i)) {
                        row_ids.push_back(components::types::logical_value_t{static_cast<int64_t>(i)});
                        bool modified = false;
                        for (const auto& expr : updates_) {
                            modified |= expr->execute(chunk, chunk, i, i, &pipeline_context->parameters);
                        }
                        if (modified) {
                            modified_->append(i);
                        } else {
                            no_modified_->append(i);
                        }
                        for (size_t j = 0; j < chunk.column_count(); j++) {
                            out_chunk.data[j].push_back(chunk.data[j].value(i));
                        }
                    }
                }
                context_->table_storage().table().update(*state, row_ids, left_->output()->data_chunk());
            }
        }
    }

} // namespace services::table::operators
