#include "gt.hpp"

bool gt::check(components::document::document_ptr document, const std::string& key, document::wrapper_value_t value) {
    return get_value(document, key) > value;
}
