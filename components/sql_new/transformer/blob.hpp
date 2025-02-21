#pragma once

#include <cstdint>
#include <cstdlib>

namespace components::sql_new::transform {
    struct blob {
        const uint8_t* data;
        size_t size;
    };
} // namespace components::sql_new::transform::expressions