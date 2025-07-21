#include "operator_max.hpp"
#include <services/collection/collection.hpp>

namespace services::table::operators::aggregate {

    constexpr auto key_result_ = "max";

    operator_max_t::operator_max_t(collection::context_collection_t* context, components::index::key_t key)
        : operator_aggregate_t(context)
        , key_(std::move(key)) {}

    components::types::logical_value_t operator_max_t::aggregate_impl() {
        if (left_ && left_->output()) {
            const auto& chunk = left_->output()->data_chunk();
            auto it = std::find_if(chunk.data.begin(), chunk.data.end(), [&](const components::vector::vector_t& v) {
                return v.type().alias() == key_.as_string();
            });
            if (it != chunk.data.end()) {
                components::types::logical_value_t max_{};
                if (chunk.size() == 0) {
                    max_.set_alias(key_result_);
                    return max_;
                } else {
                    max_ = it->value(0);
                }
                for (size_t i = 1; i < chunk.size(); i++) {
                    auto val = it->value(i);
                    if (max_ < val) {
                        std::swap(max_, val);
                    }
                }
                max_.set_alias(key_result_);
                return max_;
            }
        }
        auto result = components::types::logical_value_t(nullptr);
        result.set_alias(key_result_);
        return result;
    }

    std::string operator_max_t::key_impl() const { return key_result_; }

} // namespace services::table::operators::aggregate
