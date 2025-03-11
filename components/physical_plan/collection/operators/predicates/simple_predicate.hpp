#pragma once

#include "predicate.hpp"
#include <functional>

namespace services::collection::operators::predicates {

    class simple_predicate : public predicate {
    public:
        simple_predicate(context_collection_t* context,
                         std::function<bool(const components::document::document_ptr&,
                                            const components::document::document_ptr&,
                                            const components::logical_plan::storage_parameters*)> func);
        simple_predicate(context_collection_t* context,
                         std::vector<predicate_ptr>&& nested,
                         components::expressions::compare_type nested_type);

    private:
        bool check_impl(const components::document::document_ptr& document_left,
                        const components::document::document_ptr& document_right,
                        const components::logical_plan::storage_parameters* parameters) final;

        std::function<bool(const components::document::document_ptr&,
                           const components::document::document_ptr&,
                           const components::logical_plan::storage_parameters*)>
            func_;
        std::vector<predicate_ptr> nested_;
        components::expressions::compare_type nested_type_ = components::expressions::compare_type::invalid;
    };

    predicate_ptr create_simple_predicate(context_collection_t* context,
                                          const components::expressions::compare_expression_ptr& expr);

} // namespace services::collection::operators::predicates
