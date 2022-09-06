#pragma once

#include <services/collection/operators/operator.hpp>
#include <services/collection/operators/predicates/predicate.hpp>

namespace services::collection::operators {

    class insert final : public operator_t {
    public:
        insert(context_collection_t* collection);

    private:
        void on_execute_impl(const predicate_ptr& predicate,predicates::limit_t limit,components::cursor::sub_cursor_t* cursor);
    };

} // namespace services::operators