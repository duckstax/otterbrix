#pragma once

#include <services/collection/operators/operator.hpp>
#include <services/collection/operators/predicates/limit.hpp>

namespace services::collection::operators {

    class transfer_scan final : public read_only_operator_t {
    public:
        transfer_scan(context_collection_t* collection, predicates::limit_t limit);

    private:
        void on_execute_impl(planner::transaction_context_t* transaction_context) final;

        const predicates::limit_t limit_;
    };

} // namespace services::operators