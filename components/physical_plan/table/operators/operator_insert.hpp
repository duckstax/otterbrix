#pragma once

#include <components/physical_plan/base/operators/operator.hpp>
#include <components/vector/data_chunk.hpp>

namespace services::table::operators {

    class operator_insert final : public read_write_operator_t {
    public:
        operator_insert(collection::context_collection_t* collection);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;
    };

} // namespace services::table::operators
