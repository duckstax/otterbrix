#include "operator_get.hpp"

namespace components::collection::operators::get {

    document::value_t operator_get_t::value(const document::document_ptr& document) { return get_value_impl(document); }

} // namespace components::collection::operators::get
