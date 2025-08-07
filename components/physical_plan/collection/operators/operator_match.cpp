#include "operator_match.hpp"

namespace components::collection::operators {

    operator_match_t::operator_match_t(services::collection::context_collection_t* context,
                                       predicates::predicate_ptr predicate,
                                       logical_plan::limit_t limit)
        : read_only_operator_t(context, operator_type::match)
        , predicate_(std::move(predicate))
        , limit_(limit) {}

    void operator_match_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        int count = 0;
        if (!limit_.check(count)) {
            return; //limit = 0
        }
        if (!left_) {
            return;
        }
        if (left_->output()) {
            output_ = base::operators::make_operator_data(left_->output()->resource());
            for (size_t i = 0; i < left_->output()->documents().size(); i++) {
                if (predicate_->check(left_->output()->documents()[i],
                                      pipeline_context ? &pipeline_context->parameters : nullptr)) {
                    output_->append(left_->output()->documents()[i]);
                    ++count;
                    if (!limit_.check(count)) {
                        return;
                    }
                }
            }
        }
    }

} // namespace components::collection::operators
