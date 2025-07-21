#include "operator_avg.hpp"
#include <services/collection/collection.hpp>

namespace components::table::operators::aggregate {

    constexpr auto key_result_ = "avg";

    operator_avg_t::operator_avg_t(services::collection::context_collection_t* context, index::key_t key)
        : operator_aggregate_t(context)
        , key_(std::move(key)) {}

    types::logical_value_t operator_avg_t::aggregate_impl() {
        if (left_ && left_->output()) {
            const auto& chunk = left_->output()->data_chunk();
            auto it = std::find_if(chunk.data.begin(), chunk.data.end(), [&](const vector::vector_t& v) {
                return v.type().alias() == key_.as_string();
            });
            if (it != chunk.data.end()) {
                // TODO: sum physical values from vector insted of creating values
                types::logical_value_t sum_(it->type());
                sum_.set_alias(key_result_);
                for (size_t i = 0; i < chunk.size(); i++) {
                    // TODO: handle non summable types
                    sum_ = types::logical_value_t::sum(sum_, it->value(i));
                }
                // possible bug here since not every type could be cast to double
                auto result = types::logical_value_t(sum_.cast_as(logical_type::DOUBLE).value<double>() / chunk.size());
                result.set_alias(key_result_);
                return result;
            }
        }
        auto result = types::logical_value_t(nullptr);
        result.set_alias(key_result_);
        return result;
    }

    std::string operator_avg_t::key_impl() const { return key_result_; }

} // namespace components::table::operators::aggregate
