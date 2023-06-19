#pragma once

#include <services/collection/operators/operator.hpp>

namespace services::collection::operators {

    class operator_empty_t final : public read_only_operator_t {
    public:
        operator_empty_t(context_collection_t* context, operator_data_ptr &&data);

    private:
        void on_execute_impl(components::pipeline::context_t*) final;
    };

} // namespace services::collection::operators
