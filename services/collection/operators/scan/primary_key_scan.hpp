#pragma once

#include <services/collection/operators/operator.hpp>

namespace services::collection::operators {

    class primary_key_scan final : public read_only_operator_t {
    public:
        explicit primary_key_scan(context_collection_t* context);

        void append(document_id_t id);

    private:
        std::pmr::vector<document_id_t> ids_;

        void on_execute_impl(components::transaction::context_t* transaction_context) final;
    };

} // namespace services::operators