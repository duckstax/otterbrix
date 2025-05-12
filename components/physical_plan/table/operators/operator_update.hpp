#pragma once

#include <components/physical_plan/base/operators/operator.hpp>
#include <components/vector/data_chunk.hpp>

namespace services::table::operators {

    class operator_update final : public read_write_operator_t {
    public:
        operator_update(collection::context_collection_t* context,
                        components::vector::data_chunk_t&& update,
                        bool upsert);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;

        components::vector::data_chunk_t data_;
        bool upsert_;
    };

} // namespace services::table::operators
