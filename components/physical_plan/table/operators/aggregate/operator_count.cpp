#include "operator_count.hpp"
#include <services/collection/collection.hpp>

namespace services::table::operators::aggregate {

    constexpr auto key_result_ = "count";

    operator_count_t::operator_count_t(collection::context_collection_t* context)
        : operator_aggregate_t(context) {}

    components::types::logical_value_t operator_count_t::aggregate_impl() {
        auto resource = left_ && left_->output() ? left_->output()->resource() : context_->resource();
        auto doc = components::document::make_document(resource);
        components::types::logical_value_t result;
        if (left_ && left_->output()) {
            result = components::types::logical_value_t(uint64_t(left_->output()->size()));
        } else {
            result = components::types::logical_value_t(uint64_t(0));
        }
        result.set_alias(key_result_);
        return result;
    }

    std::string operator_count_t::key_impl() const { return key_result_; }

} // namespace services::table::operators::aggregate