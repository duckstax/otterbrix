#include "primary_key_scan.hpp"

#include <services/collection/collection.hpp>

namespace services::collection::operators {

    primary_key_scan::primary_key_scan(context_collection_t* context)
        : read_only_operator_t(context, operator_type::match) {}

    void primary_key_scan::on_execute_impl(components::pipeline::context_t*) {
        if (left_ && left_->output()) {
            output_ = base::operators::make_operator_data(context_->resource());
            for (const auto& doc : left_->output()->documents()) {
                auto it = context_->document_storage().find(get_document_id(doc));
                if (it != context_->document_storage().end()) {
                    //todo: error not unique keys
                    output_ = nullptr;
                    return;
                }
                output_->append(doc);
            }
        }
    }

} // namespace services::collection::operators
