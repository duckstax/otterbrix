#pragma once
#include <components/physical_plan/collection/operators/operator.hpp>
#include <memory>

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <string>

namespace services::collection::operators {

    class operator_drop_index final : public read_write_operator_t {
    public:
        operator_drop_index(context_collection_t* context, components::logical_plan::node_drop_index_ptr node);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;

        components::logical_plan::node_drop_index_ptr node_;
    };

} // namespace services::collection::operators
