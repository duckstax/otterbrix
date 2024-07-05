#include "operator_sum.hpp"
#include <services/collection/collection.hpp>

namespace services::collection::operators::aggregate {

    constexpr auto key_result_ = "sum";

    operator_sum_t::operator_sum_t(context_collection_t* context, components::index::key_t key)
        : operator_aggregate_t(context)
        , key_(std::move(key)) {}

    document_ptr operator_sum_t::aggregate_impl() {
        auto result = components::document::make_document(context_->resource());
        if (left_ && left_->output()) {
            const auto& documents = left_->output()->documents();
            auto tape = std::make_unique<components::document::impl::mutable_document>(context_->resource());
            components::document::value_t sum_{};
            std::for_each(documents.cbegin(), documents.cend(), [&](const document_ptr& doc) {
                sum_ = sum(sum_, get_value_from_document(doc, key_, tape.get()), tape.get(), context_->resource());
            });
            result->set(key_result_, sum_);
        } else {
            result->set(key_result_, 0);
        }
        return result;
    }

    std::string operator_sum_t::key_impl() const { return key_result_; }

} // namespace services::collection::operators::aggregate
