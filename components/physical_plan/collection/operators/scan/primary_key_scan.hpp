#pragma once

#include <components/document/document_id.hpp>
#include <components/physical_plan/collection/operators/operator.hpp>

namespace services::collection::operators {

    class primary_key_scan final : public read_only_operator_t {
    public:
        explicit primary_key_scan(context_collection_t* context);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;
    };

} // namespace services::collection::operators