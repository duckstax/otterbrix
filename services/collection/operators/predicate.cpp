#include "predicate.hpp"
#include <components/document/document_view.hpp>

document::wrapper_value_t predicate::get_value(components::document::document_ptr document, const std::string &key) {
    return document::wrapper_value_t(components::document::document_view_t(document).get_value(key));
}
