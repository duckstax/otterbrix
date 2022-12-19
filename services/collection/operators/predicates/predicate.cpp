#include "predicate.hpp"
#include <components/document/document_view.hpp>
#include "simple_predicate.hpp"

namespace services::collection::operators::predicates {

    using components::ql::condition_type;

    predicate::predicate(context_collection_t* context)
        : context_(context) {}

    bool predicate::check(const components::document::document_ptr& document) {
        return check_impl(document);
    }

    document::wrapper_value_t get_value_from_document(const components::document::document_ptr& document, const components::ql::key_t& key) {
        return document::wrapper_value_t(components::document::document_view_t(document).get_value(key.as_string()));
    }

    predicate_ptr create_predicate(context_collection_t* context, const components::expressions::compare_expression_ptr& expr) {
        auto result = create_simple_predicate(context, expr);
        if (result) {
            return result;
        }
        //todo: other predicates
        static_assert(true, "not valid condition type");
        return nullptr;
    }

} // namespace services::collection::operators::predicates
