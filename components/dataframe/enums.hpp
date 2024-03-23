#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iterator>

namespace components::dataframe {

    enum class mask_state : int32_t
    {
        unallocated,
        uninitialized,
        all_valid,
        all_null
    };

} // namespace components::dataframe