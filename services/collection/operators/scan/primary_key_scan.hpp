#pragma once

#include <components/document/document_id.hpp>
#include <services/collection/operators/operator.hpp>

namespace services::collection::operators {

    class primary_key_scan final : public read_only_operator_t {
    public:
        explicit primary_key_scan(context_collection_t* context);

        void append(components::document::document_id_t id);

    private:
        std::pmr::vector<components::document::document_id_t> ids_;

        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;
    };

} // namespace services::collection::operators