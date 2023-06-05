#pragma once
#include <components/document/core/value.hpp>
#include <components/document/core/array.hpp>

namespace components::document::structure {

    enum class attribute : uint32_t {
        type = 0,
        offset = 1,
        size = 2,
        value = 3,
        version = 4
    };

    uint32_t index_attribute(attribute attr);
    bool is_attribute(const ::document::impl::value_t *field, attribute attr);
    const ::document::impl::value_t *get_attribute(const ::document::impl::value_t *field, attribute attr);

    template <class T>
    void set_attribute(const ::document::impl::value_t *field, attribute attr, T value) {
        field->as_array()->set(index_attribute(attr), value);
    }

}
