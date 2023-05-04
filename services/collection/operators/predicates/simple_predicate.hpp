#pragma once

#include <functional>
#include "predicate.hpp"

namespace services::collection::operators::predicates {

    class simple_predicate : public predicate {
    public:
        simple_predicate(context_collection_t* context,
                         std::function<bool(const components::document::document_ptr&,
                                            const components::ql::storage_parameters*)> func);

    private:
        bool check_impl(const components::document::document_ptr& document,
                        const components::ql::storage_parameters* parameters) final;

        std::function<bool(const components::document::document_ptr&,
                           const components::ql::storage_parameters*)> func_;
    };

    predicate_ptr create_simple_predicate(context_collection_t* context,
                                          const components::expressions::compare_expression_ptr& expr);

} // namespace services::operators::predicates
