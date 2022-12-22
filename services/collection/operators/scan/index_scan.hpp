#pragma once

#include <services/collection/operators/operator.hpp>
#include <services/collection/operators/predicates/limit.hpp>

namespace services::collection::operators {

    class index_scan final : public read_only_operator_t {
    public:
        index_scan(context_collection_t* collection, components::ql::expr_ptr expr, predicates::limit_t limit);

    private:
        void on_execute_impl(planner::transaction_context_t* transaction_context) final;

        const components::ql::expr_ptr expr_;
        const predicates::limit_t limit_;
    };

} // namespace services::operators