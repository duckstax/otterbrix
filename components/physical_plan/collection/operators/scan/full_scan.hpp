#pragma once

#include <components/physical_plan/collection/operators/operator.hpp>
#include <components/physical_plan/collection/operators/predicates/predicate.hpp>
#include <components/ql/aggregate/limit.hpp>

namespace services::collection::operators {

    class full_scan final : public read_only_operator_t {
    public:
        full_scan(context_collection_t* collection, predicates::predicate_ptr predicate, components::ql::limit_t limit);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;

        const predicates::predicate_ptr predicate_;
        const components::ql::limit_t limit_;
    };

} // namespace services::collection::operators
