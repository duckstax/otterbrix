#include "operator_get.hpp"

namespace services::collection::operators::get {

    document::wrapper_value_t operator_get_t::value(const components::document::document_ptr& document) {
        return get_value_impl(document);
    }

} // namespace services::operators::get
