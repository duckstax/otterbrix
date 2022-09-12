#pragma once

#include <services/collection/operators/operator.hpp>

namespace services::collection::operators {

    class operator_delete final : public operator_t {
    public:
        explicit operator_delete(context_collection_t* collection);

    private:
        void on_execute_impl(operator_data_t* data) final;
    };

} // namespace services::operators