#pragma once

#include <components/ql/aggregate/limit.hpp>
#include <components/physical_plan/collection/operators/operator.hpp>

namespace services::collection::operators {

    class transfer_scan final : public read_only_operator_t {
    public:
        transfer_scan(context_collection_t* collection, components::ql::limit_t limit);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;

        const components::ql::limit_t limit_;
    };

} // namespace services::collection::operators
