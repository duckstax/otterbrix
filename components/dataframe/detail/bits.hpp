#pragma once

#include <cstdint>
#include <cstring>

#include <climits>
#include <dataframe/types.hpp>

namespace components::dataframe::detail {

    template<typename T>
    constexpr std::size_t size_in_bits() {
        static_assert(CHAR_BIT == 8, "Size of a byte must be 8 bits.");
        return sizeof(T) * CHAR_BIT;
    }

    constexpr inline bitmask_type set_least_significant_bits(size_type n) {
        assert(0 <= n && n < static_cast<size_type>(size_in_bits<bitmask_type>()));
        return ((bitmask_type{1} << n) - 1);
    }

    constexpr inline bitmask_type set_most_significant_bits(size_type n) {
        constexpr size_type word_size{size_in_bits<bitmask_type>()};
        assert(0 <= n && n < word_size);
        return ~((bitmask_type{1} << (word_size - n)) - 1);
    }

    constexpr inline size_type word_index(size_type bit_index) { return bit_index / size_in_bits<bitmask_type>(); }

    constexpr inline size_type intra_word_index(size_type bit_index) {
        return bit_index % size_in_bits<bitmask_type>();
    }

    bool is_set_bit(bitmask_type const* bitmask, size_type index);

    void set_bit(bitmask_type* bitmask, size_type index, bool value);

    bitmask_type funnel_shift_r(bitmask_type curr_word, bitmask_type next_word, size_type source_begin_bit);

    bitmask_type funnel_shift_l(bitmask_type curr_word, bitmask_type next_word, size_type source_end_bit);

} // namespace components::dataframe::detail