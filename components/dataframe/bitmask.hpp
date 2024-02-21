#pragma once

#include <vector>

#include <core/buffer.hpp>

#include <dataframe/enums.hpp>
#include <dataframe/types.hpp>

namespace components::dataframe {

    size_type state_null_count(mask_state state, size_type size);
    std::size_t bitmask_allocation_size_bytes(size_type number_of_bits, std::size_t padding_boundary = 64);
    size_type num_bitmask_words(size_type number_of_bits);
    core::buffer create_null_mask(std::pmr::memory_resource* resource, size_type size, mask_state state);
    void set_null_mask(bitmask_type* bitmask, size_type begin_bit, size_type end_bit, bool valid);
    core::buffer
    copy_bitmask(std::pmr::memory_resource* resource, bitmask_type const* mask, size_type begin_bit, size_type end_bit);

} // namespace components::dataframe
