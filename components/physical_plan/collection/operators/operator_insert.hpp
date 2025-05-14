#pragma once

#include <components/document/document.hpp>
#include <components/physical_plan/collection/operators/operator.hpp>

namespace services::collection::operators {

    class operator_insert final : public read_write_operator_t {
    public:
        operator_insert(context_collection_t* collection);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;
    };

} // namespace services::collection::operators
