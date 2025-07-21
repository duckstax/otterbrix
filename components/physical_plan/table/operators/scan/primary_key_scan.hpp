#pragma once

#include <components/document/document_id.hpp>
#include <components/physical_plan/base/operators/operator.hpp>

namespace components::table::operators {

    class primary_key_scan final : public read_only_operator_t {
    public:
        explicit primary_key_scan(services::collection::context_collection_t* context);

        void append(size_t id);

    private:
        vector::vector_t rows_;

        void on_execute_impl(pipeline::context_t* pipeline_context) final;
    };

} // namespace components::table::operators