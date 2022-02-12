#pragma once

namespace components::document::index {

    enum class field : uint8_t {
        type = 0,
        offset = 1,
        size = 2,
        version = 3,
        const_fields
    };

}