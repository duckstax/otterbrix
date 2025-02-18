#include "operator_min.hpp"
#include <services/collection/collection.hpp>

namespace services::collection::operators::aggregate {

    constexpr auto key_result_ = "min";

    operator_min_t::operator_min_t(context_collection_t* context, components::index::key_t key)
        : operator_aggregate_t(context)
        , key_(std::move(key)) {}

    document_ptr operator_min_t::aggregate_impl() {
        auto resource = left_ && left_->output() ? left_->output()->resource() : context_->resource();
        auto doc = components::document::make_document(resource);
        if (left_ && left_->output()) {
            const auto& documents = left_->output()->documents();
            auto min = std::min_element(documents.cbegin(),
                                        documents.cend(),
                                        [&](const document_ptr& doc1, const document_ptr& doc2) {
                                            return doc1->compare(key_.as_string(), doc2, key_.as_string()) ==
                                                   components::document::compare_t::less;
                                        });
            if (min != documents.cend()) {
                doc->set(key_result_, *min, key_.as_string());
                return doc;
            }
        }
        doc->set(key_result_, 0);
        return doc;
    }

    std::string operator_min_t::key_impl() const { return key_result_; }

} // namespace services::collection::operators::aggregate
