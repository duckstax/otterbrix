#include "operator_sum.hpp"
#include <services/collection/collection.hpp>

namespace services::table::operators::aggregate {

    constexpr auto key_result_ = "sum";

    operator_sum_t::operator_sum_t(collection::context_collection_t* context, components::index::key_t key)
        : operator_aggregate_t(context)
        , key_(std::move(key)) {}

    components::types::logical_value_t operator_sum_t::aggregate_impl() {
        if (left_ && left_->output()) {
            const auto& chunk = left_->output()->data_chunk();
            auto it = std::find_if(chunk.data.begin(), chunk.data.end(), [&](const components::vector::vector_t& v) {
                return v.type().alias() == key_.as_string();
            });
            if (it != chunk.data.end()) {
                // TODO: sum physical values from vector insted of creating values
                components::types::logical_value_t sum_(it->type());
                sum_.set_alias(key_result_);
                for (size_t i = 0; i < it->size(); i++) {
                    // TODO: handle non summable types
                    sum_ = components::types::logical_value_t::sum(sum_, it->value(i));
                }
                return sum_;
            }
        }
        auto result = components::types::logical_value_t(nullptr);
        result.set_alias(key_result_);
        return result;
    }

    std::string operator_sum_t::key_impl() const { return key_result_; }

} // namespace services::table::operators::aggregate
