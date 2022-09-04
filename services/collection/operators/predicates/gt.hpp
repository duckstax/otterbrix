#pragma once

#include "predicate.hpp"

namespace services::collection::operators {

    class gt : public predicate {
    public:
        gt(context_collection_t* context, components::ql::key_t key, components::ql::expr_value_t value);

        bool check_impl(const components::document::document_ptr& document) final;

    private:
        const components::ql::key_t key_;
        const components::ql::expr_value_t value_;
    };

} // namespace services::operators
