#pragma once

#include <collection/operators/predicates/predicate.hpp>

#include "collection/operators/predicates/limit.hpp"
#include <services/collection/collection.hpp>

namespace services::collection::operators {

    class operator_t {
    public:
        operator_t(context_collection_t* collection);
        virtual ~operator_t() = default;

        void on_execute(const predicate_ptr&, predicates::limit_t, components::cursor::sub_cursor_t*);

    protected:
        context_collection_t* context_;

    private:
        virtual void on_execute_impl(const predicate_ptr&, predicates::limit_t, components::cursor::sub_cursor_t*) = 0;
    };

    using operator_ptr = std::unique_ptr<operator_t>;

} // namespace services::collection::operators
