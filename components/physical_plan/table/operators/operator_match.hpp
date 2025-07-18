#pragma once

#include <components/physical_plan/base/operators/operator.hpp>

#include <components/logical_plan/node_limit.hpp>
#include <expressions/compare_expression.hpp>

namespace services::table::operators {

    class operator_match_t final : public read_only_operator_t {
    public:
        operator_match_t(collection::context_collection_t* context,
                         const components::expressions::compare_expression_ptr& expression,
                         components::logical_plan::limit_t limit);

    private:
        const components::expressions::compare_expression_ptr expression_;
        const components::logical_plan::limit_t limit_;

        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;
    };

} // namespace services::table::operators
