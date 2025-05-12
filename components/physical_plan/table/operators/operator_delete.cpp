#include "operator_delete.hpp"
#include <services/collection/collection.hpp>

namespace services::table::operators {

    operator_delete::operator_delete(collection::context_collection_t* context)
        : read_write_operator_t(context, operator_type::remove) {}

    void operator_delete::on_execute_impl(components::pipeline::context_t* pipeline_context) {
        modified_ = base::operators::make_operator_write_data<size_t>(context_->resource());
        auto& chunk = left_->output()->data_chunk();
        auto state = context_->table_storage().table().initialize_delete({});
        context_->table_storage().table().delete_rows(*state, chunk.data[0], chunk.size());

        for (size_t i = 0; i < chunk.data[0].size(); i++) {
            size_t id = *chunk.data[0].data<int64_t>();
            modified_->append(id);
            // TODO: delete from index
            //context_->index_engine()->delete_document(document, pipeline_context);
        }
    }

} // namespace services::table::operators
