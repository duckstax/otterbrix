#include "operator_insert.hpp"
#include <services/collection/collection.hpp>

namespace services::table::operators {

    operator_insert::operator_insert(collection::context_collection_t* context)
        : read_write_operator_t(context, operator_type::insert) {}

    void operator_insert::on_execute_impl(components::pipeline::context_t* pipeline_context) {
        if (left_ && left_->output()) {
            modified_ = base::operators::make_operator_write_data<size_t>(context_->resource());
            output_ = base::operators::make_operator_data(context_->resource(),
                                                          left_->output()->data_chunk().types(),
                                                          left_->output()->data_chunk().size());
            components::table::table_append_state state(context_->resource());
            context_->table_storage().table().append_lock(state);
            context_->table_storage().table().initialize_append(state);
            for (size_t id = 0; id < left_->output()->data_chunk().size(); id++) {
                modified_->append(id + state.row_start);
                // TODO: insert into index
            }
            context_->table_storage().table().append(left_->output()->data_chunk(), state);
            context_->table_storage().table().finalize_append(state);
            left_->output()->data_chunk().copy(output_->data_chunk(), 0);
        }
    }

} // namespace services::table::operators
