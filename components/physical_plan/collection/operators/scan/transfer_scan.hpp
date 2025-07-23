#pragma once

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/base/operators/operator.hpp>

namespace components::collection::operators {

    class transfer_scan final : public read_only_operator_t {
    public:
        transfer_scan(services::collection::context_collection_t* collection, logical_plan::limit_t limit);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) final;

        const logical_plan::limit_t limit_;
    };

} // namespace components::collection::operators
