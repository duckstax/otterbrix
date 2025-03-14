#include "operator_match.hpp"

namespace services::collection::operators {

    operator_match_t::operator_match_t(context_collection_t* context,
                                       predicates::predicate_ptr predicate,
                                       components::logical_plan::limit_t limit)
        : read_only_operator_t(context, operator_type::match)
        , predicate_(std::move(predicate))
        , limit_(limit) {}

    void operator_match_t::on_execute_impl(components::pipeline::context_t* pipeline_context) {
        int count = 0;
        if (!limit_.check(count)) {
            return; //limit = 0
        }
        if (!left_) {
            return;
        }
        if (left_->output()) {
            output_ = make_operator_data(left_->output()->resource());
            for (auto& doc : left_->output()->documents()) {
                if (predicate_->check(doc, pipeline_context ? &pipeline_context->parameters : nullptr)) {
                    output_->append(doc);
                    ++count;
                    if (!limit_.check(count)) {
                        return;
                    }
                }
            }
        }
    }

} // namespace services::collection::operators
