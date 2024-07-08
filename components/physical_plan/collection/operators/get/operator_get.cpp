#include "operator_get.hpp"

namespace services::collection::operators::get {

    components::document::value_t operator_get_t::value(const components::document::document_ptr& document,
                                                        components::document::impl::base_document* tape) {
        return get_value_impl(document, tape);
    }

} // namespace services::collection::operators::get
