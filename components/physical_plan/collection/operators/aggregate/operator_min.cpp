#include "operator_min.hpp"
#include <services/collection/collection.hpp>

namespace services::collection::operators::aggregate {

    constexpr auto key_result_ = "min";

    operator_min_t::operator_min_t(context_collection_t* context, components::index::key_t key)
        : operator_aggregate_t(context)
        , key_(std::move(key)) {}

    document_ptr operator_min_t::aggregate_impl() {
        auto doc = components::document::make_document(context_->resource());
        if (left_ && left_->output()) {
            const auto& documents = left_->output()->documents();
            auto min =
                std::min_element(documents.cbegin(),
                                 documents.cend(),
                                 [&](const document_ptr& doc1, const document_ptr& doc2) {
                                     return get_value_from_document(doc1, key_) < get_value_from_document(doc2, key_);
                                 });
            if (min != documents.cend()) {
                doc->set(key_result_, get_value_from_document(*min, key_));
            }
        }
        doc->set(key_result_, 0);
        return doc;
    }

    std::string operator_min_t::key_impl() const { return key_result_; }

} // namespace services::collection::operators::aggregate
