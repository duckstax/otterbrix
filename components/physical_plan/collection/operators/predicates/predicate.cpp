#include "predicate.hpp"
#include "simple_predicate.hpp"
#include <components/document/document.hpp>

namespace services::collection::operators::predicates {

    bool predicate::check(const components::document::document_ptr& document,
                          const components::logical_plan::storage_parameters* parameters) {
        return check_impl(document, nullptr, parameters);
    }
    bool predicate::check(const components::document::document_ptr& document_left,
                          const components::document::document_ptr& document_right,
                          const components::logical_plan::storage_parameters* parameters) {
        return check_impl(document_left, document_right, parameters);
    }

    predicate_ptr create_predicate(const components::expressions::compare_expression_ptr& expr) {
        auto result = create_simple_predicate(expr);
        if (result) {
            return result;
        }
        //todo: other predicates
        static_assert(true, "not valid condition type");
        return nullptr;
    }

} // namespace services::collection::operators::predicates
