#pragma once

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/base/operators/operator.hpp>

namespace services::table::operators {

    class transfer_scan final : public read_only_operator_t {
    public:
        transfer_scan(collection::context_collection_t* collection, components::logical_plan::limit_t limit);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;

        const components::logical_plan::limit_t limit_;
    };

} // namespace services::table::operators
