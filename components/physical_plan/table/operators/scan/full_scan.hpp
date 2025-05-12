#pragma once

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/base/operators/operator.hpp>
#include <components/table/column_state.hpp>
#include <expressions/compare_expression.hpp>

namespace services::table::operators {

    std::unique_ptr<components::table::table_filter_t>
    transform_predicate(const components::expressions::compare_expression_ptr& exresssion);

    class full_scan final : public read_only_operator_t {
    public:
        full_scan(collection::context_collection_t* collection,
                  const components::expressions::compare_expression_ptr& exresssion,
                  components::logical_plan::limit_t limit);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;

        components::expressions::compare_expression_ptr exresssion_;
        const components::logical_plan::limit_t limit_;
    };

} // namespace services::table::operators
