#include "primary_key_scan.hpp"

#include <services/collection/collection.hpp>

namespace services::collection::operators {

    primary_key_scan::primary_key_scan(context_collection_t* context)
        : read_only_operator_t(context, operator_type::match)
        , ids_(context->resource()) {
    }

    void primary_key_scan::append(document_id_t id) {
        ids_.push_back(id);
    }

    void primary_key_scan::on_execute_impl(planner::transaction_context_t* transaction_context) {
        output_ = make_operator_data(context_->resource());
        for (const auto &id : ids_) {
            auto it = context_->storage().find(id);
            if (it == context_->storage().end()) {
                output_->append(it->second);
            }
        }
    }

} // namespace services::collection::operators
