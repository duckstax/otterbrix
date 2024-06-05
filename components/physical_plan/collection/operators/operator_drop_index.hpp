#pragma once
#include <components/ql/index.hpp>
#include <memory>
#include <components/physical_plan/collection/operators/operator.hpp>

#include <string>

namespace services::collection::operators {

    class operator_drop_index final : public read_write_operator_t {
    public:
        operator_drop_index(context_collection_t* context, components::ql::drop_index_t* ql);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;

        components::ql::drop_index_t* ql_;
    };

} // namespace services::collection::operators
