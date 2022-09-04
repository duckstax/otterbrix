#pragma once

#include "operator.hpp"
#include <services/collection/operators/predicates/predicate.hpp>

namespace services::collection::operators {

    class full_scan final : public operator_t {
    public:
        full_scan(collection_t* collection);

    private:
        void on_execute_impl(predicate_ptr predicate,limit_t limit,components::cursor::sub_cursor_t* cursor);
    };

} // namespace services::operators