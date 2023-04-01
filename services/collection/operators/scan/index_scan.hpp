#pragma once

#include <services/collection/operators/operator.hpp>
#include <services/collection/operators/predicates/limit.hpp>

namespace services::collection::operators {

    class index_scan final : public read_only_operator_t {
    public:
        index_scan(context_collection_t* collection, components::expressions::compare_expression_ptr expr, predicates::limit_t limit);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;

        const components::expressions::compare_expression_ptr expr_;
        const predicates::limit_t limit_;
    };

} // namespace services::operators
