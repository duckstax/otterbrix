#pragma once

#include <services/collection/operators/operator.hpp>

namespace services::collection::operators {

    class operator_delete final : public operator_t {
    public:
        operator_delete(context_collection_t* collection);

    private:
        void on_execute_impl(components::cursor::sub_cursor_t* cursor) final;
    };

} // namespace services::operators