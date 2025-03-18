#pragma once

#include <components/physical_plan/collection/operators/operator.hpp>

#include "predicates/predicate.hpp"

#include <components/logical_plan/node_limit.hpp>

namespace services::collection::operators {

    class operator_match_t final : public read_only_operator_t {
    public:
        operator_match_t(context_collection_t* context,
                         predicates::predicate_ptr predicate,
                         components::logical_plan::limit_t limit);

    private:
        const predicates::predicate_ptr predicate_;
        const components::logical_plan::limit_t limit_;

        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;
    };

} // namespace services::collection::operators
