#pragma once
#include <components/physical_plan/collection/operators/operator.hpp>
#include <components/ql/index.hpp>
#include <memory>
#include <string>

namespace services::collection::operators {

    class operator_add_index final : public read_write_operator_t {
    public:
        operator_add_index(context_collection_t* context, components::ql::create_index_ptr ql);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;

        components::ql::create_index_ptr index_ql_;
    };

} // namespace services::collection::operators
