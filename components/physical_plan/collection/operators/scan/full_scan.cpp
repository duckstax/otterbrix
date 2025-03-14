#include "full_scan.hpp"

#include <services/collection/collection.hpp>

namespace services::collection::operators {

    full_scan::full_scan(context_collection_t* context,
                         predicates::predicate_ptr predicate,
                         components::logical_plan::limit_t limit)
        : read_only_operator_t(context, operator_type::match)
        , predicate_(std::move(predicate))
        , limit_(limit) {}

    void full_scan::on_execute_impl(components::pipeline::context_t* pipeline_context) {
        trace(context_->log(), "full_scan");
        int count = 0;
        if (!limit_.check(count)) {
            return; //limit = 0
        }
        output_ = make_operator_data(context_->resource());
        for (auto& it : context_->storage()) {
            if (predicate_->check(it.second, pipeline_context ? &pipeline_context->parameters : nullptr)) {
                output_->append(it.second);
                ++count;
                if (!limit_.check(count)) {
                    return;
                }
            }
        }
    }

} // namespace services::collection::operators
