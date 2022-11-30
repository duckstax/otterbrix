#include "operator_min.hpp"

namespace services::collection::operators::aggregate {

    constexpr auto key_result_ = "min";

    operator_min_t::operator_min_t(context_collection_t *context, components::index::key_t key)
        : operator_aggregate_t(context)
        , key_(std::move(key)) {
    }

    document_ptr operator_min_t::aggregate_impl() {
        if (left_ && left_->output()) {
            const auto &documents = left_->output()->documents();
            auto min = std::min_element(documents.cbegin(), documents.cend(), [&](const document_ptr &doc1, const document_ptr &doc2) {
                return components::ql::get_value(doc1, key_) < components::ql::get_value(doc2, key_);
            });
            if (min != documents.cend()) {
                return components::document::make_document(key_result_, *components::ql::get_value(*min, key_));
            }
        }
        return components::document::make_document(key_result_, 0);
    }

    std::string operator_min_t::key_impl() const {
        return key_result_;
    }

} // namespace services::collection::operators::aggregate