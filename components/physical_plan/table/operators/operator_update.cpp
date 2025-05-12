#include "operator_update.hpp"
#include <services/collection/collection.hpp>

namespace services::table::operators {

    operator_update::operator_update(collection::context_collection_t* context,
                                     components::vector::data_chunk_t&& update,
                                     bool upsert)
        : read_write_operator_t(context, operator_type::update)
        , data_(std::move(update))
        , upsert_(upsert) {}

    void operator_update::on_execute_impl(components::pipeline::context_t* pipeline_context) {
        if (left_ && left_->output()) {
            // TODO: update indexes
            if (left_->output()->size() == 0) {
                if (upsert_) {
                    output_ = base::operators::make_operator_data(context_->resource(),
                                                                  left_->output()->data_chunk().types());

                    components::table::table_append_state state(context_->resource());
                    context_->table_storage().table().initialize_append(state);
                    context_->table_storage().table().append(output_->data_chunk(), state);
                }
            } else {
                modified_ = base::operators::make_operator_write_data<size_t>(context_->resource());
                no_modified_ = base::operators::make_operator_write_data<size_t>(context_->resource());
                auto state = context_->table_storage().table().initialize_update({});
                components::vector::vector_t row_ids(context_->resource(), logical_type::BIGINT);
                // TODO: find rows to update
                context_->table_storage().table().update(*state, row_ids, data_);
                // TODO: fill modified_ and no_modified_
            }
        }
    }

} // namespace services::table::operators
