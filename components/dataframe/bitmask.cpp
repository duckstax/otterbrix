#include "bitmask.hpp"

#include "core/scalar.hpp"

#include "dataframe/detail/bits.hpp"
#include <dataframe/detail/bitmask.hpp>

namespace components::dataframe {

    size_type state_null_count(mask_state state, size_type size) { return detail::state_null_count(state, size); }

    size_type num_bitmask_words(size_type number_of_bits) { return detail::num_bitmask_words(number_of_bits); }

    core::buffer create_null_mask(std::pmr::memory_resource* mr, size_type size, mask_state state) {
        return detail::create_null_mask(mr, size, state);
    }

    std::size_t bitmask_allocation_size_bytes(size_type number_of_bits, std::size_t padding_boundary) {
        return detail::bitmask_allocation_size_bytes(number_of_bits, padding_boundary);
    }

    void set_null_mask(bitmask_type* bitmask, size_type begin_bit, size_type end_bit, bool valid) {
        return detail::set_null_mask(bitmask, begin_bit, end_bit, valid);
    }

    core::buffer copy_bitmask(std::pmr::memory_resource* resource,
                              bitmask_type const* mask,
                              size_type begin_bit,
                              size_type end_bit) {
        return detail::copy_bitmask(resource, mask, begin_bit, end_bit);
    }

} // namespace components::dataframe
