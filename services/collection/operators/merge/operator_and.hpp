#pragma once

#include "operator_merge.hpp"

namespace services::collection::operators::merge {

    class operator_and_t : public operator_merge_t {
    public:
        explicit operator_and_t(context_collection_t* context, predicates::limit_t limit);

    private:
        void on_merge_impl(planner::transaction_context_t* transaction_context) final;
    };

} // namespace services::collection::operators::merge
