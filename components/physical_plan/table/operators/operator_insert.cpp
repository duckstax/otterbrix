#include "operator_insert.hpp"
#include <services/collection/collection.hpp>

namespace services::table::operators {

    operator_insert::operator_insert(collection::context_collection_t* context, components::vector::data_chunk_t&& data)
        : read_write_operator_t(context, operator_type::insert)
        , data_(std::move(data)) {}

    operator_insert::operator_insert(collection::context_collection_t* context,
                                     const components::vector::data_chunk_t& data)
        : read_write_operator_t(context, operator_type::insert)
        , data_(context->resource(), data.types(), data.size()) {
        data.copy(data_, 0);
    }

    void operator_insert::on_execute_impl(components::pipeline::context_t* pipeline_context) {
        modified_ = base::operators::make_operator_write_data<size_t>(context_->resource());
        components::table::table_append_state state(context_->resource());
        context_->table_storage().table().initialize_append(state);
        for (size_t id = 0; id < data_.size(); id++) {
            modified_->append(id + state.row_start);
            // TODO: insert into index
        }
        context_->table_storage().table().append(data_, state);
        output_ = base::operators::make_operator_data(context_->resource(), data_.types(), data_.size());
        data_.copy(output_->data_chunk(), 0);
    }

} // namespace services::table::operators
