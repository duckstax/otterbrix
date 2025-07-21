#include "simple_value.hpp"

namespace services::collection::operators::get {

    operator_get_ptr simple_value_t::create(const components::expressions::key_t& key) {
        return operator_get_ptr(new simple_value_t(key));
    }

    simple_value_t::simple_value_t(const components::expressions::key_t& key)
        : operator_get_t()
        , key_(key) {}

    components::document::value_t simple_value_t::get_value_impl(const document_ptr& document) {
        return document->get_value(key_.as_string());
    }

} // namespace services::collection::operators::get
