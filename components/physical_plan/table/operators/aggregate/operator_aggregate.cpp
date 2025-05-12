#include "operator_aggregate.hpp"
#include <services/collection/collection.hpp>

namespace services::table::operators::aggregate {

    operator_aggregate_t::operator_aggregate_t(collection::context_collection_t* context)
        : read_only_operator_t(context, operator_type::aggregate)
        , aggregate_result_(context->resource()) {}

    void operator_aggregate_t::on_execute_impl(components::pipeline::context_t*) {
        aggregate_result_.emplace_back(aggregate_impl());
    }

    void operator_aggregate_t::set_value(std::pmr::vector<components::types::logical_value_t>& row,
                                         std::string_view key) const {
        auto it =
            std::find_if(aggregate_result_.begin(),
                         aggregate_result_.end(),
                         [&](const components::types::logical_value_t& v) { return v.type().alias() == key_impl(); });
        components::types::logical_value_t val{nullptr};
        if (it == aggregate_result_.end()) {
            val = *it;
        }
        auto res_it = std::find_if(row.begin(), row.end(), [&](const components::types::logical_value_t& v) {
            return v.type().alias() == key_impl();
        });
        if (res_it == row.end()) {
            row.emplace_back(std::move(val));
        } else {
            *res_it = std::move(val);
        }
    }

    components::types::logical_value_t operator_aggregate_t::value() const {
        auto it =
            std::find_if(aggregate_result_.begin(),
                         aggregate_result_.end(),
                         [&](const components::types::logical_value_t& v) { return v.type().alias() == key_impl(); });
        if (it == aggregate_result_.end()) {
            return components::types::logical_value_t{nullptr};
        }
        return *it;
    }
} // namespace services::table::operators::aggregate
