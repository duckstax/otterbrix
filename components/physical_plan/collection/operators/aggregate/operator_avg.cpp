#include "operator_avg.hpp"
#include <services/collection/collection.hpp>

namespace services::collection::operators::aggregate {

    constexpr auto key_result_ = "avg";

    operator_avg_t::operator_avg_t(context_collection_t* context, components::index::key_t key)
        : operator_aggregate_t(context)
        , key_(std::move(key)) {}

    document_ptr operator_avg_t::aggregate_impl() {
        auto resource = left_ && left_->output() ? left_->output()->resource() : context_->resource();
        auto result = components::document::make_document(resource);
        auto tape = std::make_unique<components::document::impl::base_document>(resource);
        if (left_ && left_->output()) {
            const auto& documents = left_->output()->documents();
            if (!documents.empty()) {
                components::document::value_t sum_{};
                std::for_each(documents.cbegin(), documents.cend(), [&](const document_ptr& doc) {
                    sum_ = sum(sum_, get_value_from_document(doc, key_), tape.get(), resource);
                });
                result->set(key_result_, sum_.as_double() / double(documents.size()));
                return result;
            }
        }
        result->set(key_result_, 0);
        return result;
    }

    std::string operator_avg_t::key_impl() const { return key_result_; }

} // namespace services::collection::operators::aggregate
