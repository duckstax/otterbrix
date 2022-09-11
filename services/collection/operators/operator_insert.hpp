#pragma once

#include <services/collection/operators/operator.hpp>

namespace services::collection::operators {

    class operator_insert final : public operator_t {
    public:
        operator_insert(context_collection_t* collection, std::pmr::vector<document_ptr>&& documents);
        operator_insert(context_collection_t* collection, const std::pmr::vector<document_ptr>& documents);

    private:
        void on_execute_impl(components::cursor::sub_cursor_t* cursor) final;

        std::pmr::vector<document_ptr> documents_;
    };

} // namespace services::operators