#pragma once

#include "collection/operators/predicates/predicate.hpp"
#include "limit.hpp"
#include <services/collection/collection.hpp>

namespace services::collection::operators {

    class operator_t  {
    public:

        operator_t(collection_t* collection);
        virtual ~operator_t() = default;

        void on_execute(predicate_ptr,limit_t,components::cursor::sub_cursor_t*);

    protected:
        const context_collection_t* collection_;

    private:
        virtual void on_execute_impl(predicate_ptr,limit_t,components::cursor::sub_cursor_t* ) = 0;
    };

    using operator_ptr = std::unique_ptr<operator_t>;

} // namespace services::operators
