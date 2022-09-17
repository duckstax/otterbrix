#include "operator_delete.hpp"

namespace services::collection::operators {

    operator_delete::operator_delete(context_collection_t* context)
        : read_write_operator_t(context, operator_type::remove) {
    }

    void operator_delete::on_execute_impl(planner::transaction_context_t* transaction_context) {
        if (left_ && left_->output()) {
            for (const auto& document : left_->output()->documents()) {
                context_->storage().erase(context_->storage().find(get_document_id(document)));
                context_->index_engine()->delete_document(document);
            }
        }
    }

} // namespace services::collection::operators
