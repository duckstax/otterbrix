#include "operator_min.hpp"
#include <services/collection/collection.hpp>

namespace services::table::operators::aggregate {

    constexpr auto key_result_ = "min";

    operator_min_t::operator_min_t(collection::context_collection_t* context, components::index::key_t key)
        : operator_aggregate_t(context)
        , key_(std::move(key)) {}

    components::types::logical_value_t operator_min_t::aggregate_impl() {
        if (left_ && left_->output()) {
            const auto& chunk = std::get<components::vector::data_chunk_t>(left_->output()->data());
            auto it = std::find_if(chunk.data.begin(), chunk.data.end(), [&](const components::vector::vector_t& v) {
                return v.type().alias() == key_.as_string();
            });
            if (it != chunk.data.end()) {
                components::types::logical_value_t min_(it->type());
                min_.set_alias(key_result_);
                for (size_t i = 0; i < it->size(); i++) {
                    auto val = it->value(i);
                    if (min_ > val) {
                        std::swap(min_, val);
                    }
                }
                return min_;
            }
        }
        auto result = components::types::logical_value_t(nullptr);
        result.set_alias(key_result_);
        return result;
    }

    std::string operator_min_t::key_impl() const { return key_result_; }

} // namespace services::table::operators::aggregate
