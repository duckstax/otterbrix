#include "operator_or.hpp"

namespace services::collection::operators::merge {

    operator_or_t::operator_or_t(context_collection_t* context, predicates::limit_t limit)
        : operator_merge_t(context, limit) {
    }

    void operator_or_t::on_merge_impl(planner::transaction_context_t*) {
        //todo: optimize merge
        int count = 0;
        if (!limit_.check(count)) {
            return; //limit = 0
        }
        if (left_ && right_ && left_->output() && right_->output()) {
            output_ = make_operator_data(context_->resource());
            for (const auto &document : left_->output()->documents()) {
                output_->append(document);
                ++count;
                if (!limit_.check(count)) {
                    return;
                }
            }
            for (const auto &document : right_->output()->documents()) {
                auto it = std::find_if(output_->documents().cbegin(), output_->documents().cend(), [&document](const document_ptr &doc) {
                    return get_document_id(doc) == get_document_id(document);
                });
                if (it == output_->documents().cend()) {
                    output_->append(document);
                    ++count;
                    if (!limit_.check(count)) {
                        return;
                    }
                }
            }
        }
    }

} // namespace services::collection::operators::merge
