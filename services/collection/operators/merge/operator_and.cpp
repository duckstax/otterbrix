#include "operator_and.hpp"

namespace services::collection::operators::merge {

    operator_and_t::operator_and_t(context_collection_t* context)
        : operator_merge_t(context) {
    }

    void operator_and_t::on_merge_impl(planner::transaction_context_t* transaction_context) {
        //todo: optimize merge
        if (left_ && right_ && left_->output() && right_->output()) {
            output_ = make_operator_data(context_->resource());
            const auto &right_documents = right_->output()->documents();
            for (const auto &left_document : left_->output()->documents()) {
                auto it = std::find_if(right_documents.cbegin(), right_documents.cend(), [&left_document](const document_ptr &doc) {
                    return get_document_id(doc) == get_document_id(left_document);
                });
                if (it != right_documents.cend()) {
                    output_->append(left_document);
                }
            }
        }
    }

} // namespace services::collection::operators::merge