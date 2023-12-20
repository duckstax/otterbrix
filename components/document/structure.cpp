#include "structure.hpp"

namespace components::document::structure {

    uint32_t index_attribute(attribute attr) { return static_cast<uint32_t>(attr); }

    bool is_attribute(const ::document::impl::value_t* field, attribute attr) {
        return field->as_array()->count() >= index_attribute(attr);
    }

    const ::document::impl::value_t* get_attribute(const ::document::impl::value_t* field, attribute attr) {
        return field->as_array()->get(index_attribute(attr));
    }

} // namespace components::document::structure