#pragma once

#include <services/collection/operators/operator.hpp>

namespace services::collection::operators {

    class operator_update final : public operator_t {
    public:
        operator_update(context_collection_t* context, document_ptr update);

    private:
        void on_execute_impl(operator_data_t* data) final;

        document_ptr update_;
    };

} // namespace services::operators