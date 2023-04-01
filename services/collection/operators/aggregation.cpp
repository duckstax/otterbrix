#include "aggregation.hpp"

#include <collection/operators/predicates/limit.hpp>
#include <collection/operators/scan/transfer_scan.hpp>

namespace services::collection::operators {

    aggregation::aggregation(context_collection_t* context)
        : read_only_operator_t(context, operator_type::aggregate) {}

    void aggregation::set_match(operator_ptr&& match) {
        match_ = std::move(match);
    }

    void aggregation::set_group(operator_ptr&& group) {
        group_ = std::move(group);
    }

    void aggregation::set_sort(operator_ptr&& sort) {
        sort_ = std::move(sort);
    }

    void aggregation::on_execute_impl(components::pipeline::context_t *pipeline_context) {
        operator_ptr executor = match_
                ? std::move(match_)
                : static_cast<operator_ptr>(std::make_unique<transfer_scan>(context_, predicates::limit_t::unlimit()));
        if (group_) {
            group_->set_children(std::move(executor));
            executor = std::move(group_);
        }
        if (sort_) {
            sort_->set_children(std::move(executor));
            executor = std::move(sort_);
        }
        executor->on_execute(pipeline_context);
        take_output(executor);
    }

} // namespace services::collection::operators
