#pragma once

#include <components/physical_plan/base/operators/operator.hpp>
#include <components/physical_plan/collection/operators/predicates/predicate.hpp>

namespace services::table::operators {

    class operator_delete final : public read_write_operator_t {
    public:
        explicit operator_delete(collection::context_collection_t* collection,
                                 components::expressions::compare_expression_ptr expr);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;

        components::expressions::compare_expression_ptr compare_expression_;
    };

} // namespace services::table::operators