#pragma once
#include <components/logical_plan/node_create_index.hpp>
#include <components/physical_plan/base/operators/operator.hpp>
#include <memory>
#include <string>

namespace services::base::operators {

    class operator_add_index final : public read_write_operator_t {
    public:
        operator_add_index(collection::context_collection_t* context,
                           components::logical_plan::node_create_index_ptr node);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;

        components::logical_plan::node_create_index_ptr index_node_;
    };

} // namespace services::base::operators
