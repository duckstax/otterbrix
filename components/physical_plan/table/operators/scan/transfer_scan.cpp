#include "transfer_scan.hpp"

#include <services/collection/collection.hpp>

namespace services::table::operators {

    transfer_scan::transfer_scan(collection::context_collection_t* context, components::logical_plan::limit_t limit)
        : read_only_operator_t(context, operator_type::match)
        , limit_(limit) {}

    void transfer_scan::on_execute_impl(components::pipeline::context_t*) {
        trace(context_->log(), "transfer_scan");
        int count = 0;
        if (!limit_.check(count)) {
            return; //limit = 0
        }

        auto types = context_->table_storage().table().copy_types();
        output_ = base::operators::make_operator_data(context_->resource(), types);
        std::vector<components::table::storage_index_t> column_indices;
        column_indices.reserve(context_->table_storage().table().column_count());
        for (int64_t i = 0; i < context_->table_storage().table().column_count(); i++) {
            column_indices.emplace_back(i);
        }
        components::table::table_scan_state state(std::pmr::get_default_resource());
        context_->table_storage().table().initialize_scan(state, column_indices);
        // TODO: check limit inside scan
        context_->table_storage().table().scan(output_->data_chunk(), state);
    }

} // namespace services::table::operators
