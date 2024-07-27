#pragma once

#include <components/document/document.hpp>
#include <components/physical_plan/collection/operators/operator.hpp>

namespace services::collection::operators {

    class operator_update final : public read_write_operator_t {
    public:
        operator_update(context_collection_t* context, components::document::document_ptr update, bool upsert);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;

        components::document::document_ptr update_;
        bool upsert_;
    };

} // namespace services::collection::operators
