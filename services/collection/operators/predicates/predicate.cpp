#include "predicate.hpp"
#include <components/document/document_view.hpp>

namespace services::collection::operators {

    predicate::predicate(const context_t& context)
        : context_(context) {}

    bool predicate::check(const components::document::document_ptr& document) {
        return check_impl(document);
    }

    document::wrapper_value_t get_value_from_document(const components::document::document_ptr& document, const components::ql::key_t& key) {
        return document::wrapper_value_t(components::document::document_view_t(document).get_value(key.as_string()));
    }

} // namespace services::operators
