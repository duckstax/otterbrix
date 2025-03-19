#pragma once

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/collection/operators/operator.hpp>

namespace services::collection::operators {

    class index_scan final : public read_only_operator_t {
    public:
        index_scan(context_collection_t* collection,
                   components::expressions::compare_expression_ptr expr,
                   components::logical_plan::limit_t limit);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;
        void on_resume_impl(components::pipeline::context_t* pipeline_context) final;

        const components::expressions::compare_expression_ptr expr_;
        const components::logical_plan::limit_t limit_;
    };

} // namespace services::collection::operators
