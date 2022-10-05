#include "operator_aggregate.hpp"

namespace services::collection::operators::aggregate {

    operator_aggregate_t::operator_aggregate_t(context_collection_t* context)
        : read_only_operator_t(context, operator_type::aggregate) {
    }

    void operator_aggregate_t::on_execute_impl(planner::transaction_context_t* transaction_context) {
        output_ = make_operator_data(context_->resource());
        output_->append(aggregate_impl());
    }

    document::wrapper_value_t operator_aggregate_t::value() const {
        return document::wrapper_value_t(document_view_t(output_->documents().at(0)).get_value(key_impl()));
    }

} // namespace services::collection::operators::aggregate