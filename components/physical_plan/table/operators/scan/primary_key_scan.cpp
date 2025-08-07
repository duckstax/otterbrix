#include "primary_key_scan.hpp"

#include <services/collection/collection.hpp>

namespace components::table::operators {

    primary_key_scan::primary_key_scan(services::collection::context_collection_t* context)
        : read_only_operator_t(context, operator_type::match)
        , rows_(context->resource(), logical_type::BIGINT) {}

    void primary_key_scan::append(size_t id) {
        rows_.set_value(size_++, types::logical_value_t(static_cast<int64_t>(id)));
    }

    void primary_key_scan::on_execute_impl(pipeline::context_t*) {
        auto types = context_->table_storage().table().copy_types();
        output_ = base::operators::make_operator_data(context_->resource(), types);
        table::column_fetch_state state;
        std::vector<table::storage_index_t> column_indices;
        column_indices.reserve(context_->table_storage().table().column_count());
        for (int64_t i = 0; i < context_->table_storage().table().column_count(); i++) {
            column_indices.emplace_back(i);
        }
        context_->table_storage().table().fetch(output_->data_chunk(), column_indices, rows_, rows_.size(), state);
    }

} // namespace components::table::operators
