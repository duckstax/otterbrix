#pragma once

#include <services/collection/operators/operator.hpp>

namespace services::collection::operators {

    class operator_insert final : public read_write_operator_t {
    public:
        operator_insert(context_collection_t* collection, std::pmr::vector<document_ptr>&& documents);
        operator_insert(context_collection_t* collection, const std::pmr::vector<document_ptr>& documents);

    private:
        void on_execute_impl(components::transaction::context_t* transaction_context) final;

        std::pmr::vector<document_ptr> documents_;
    };

} // namespace services::operators