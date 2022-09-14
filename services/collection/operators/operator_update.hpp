#pragma once

#include <services/collection/operators/operator.hpp>

namespace services::collection::operators {

    class operator_update final : public read_write_operator_t {
    public:
        operator_update(context_collection_t* context, document_ptr update);

    private:
        void on_execute_impl(planner::transaction_context_t* transaction_context) final;

        document_ptr update_;
    };

} // namespace services::operators