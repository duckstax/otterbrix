#include "bits.hpp"

namespace components::dataframe::detail {

    bool is_set_bit(bitmask_type const* bitmask, size_type index) {
        auto index_word = word_index(index);
        auto index_bit = intra_word_index(index);
        return *(bitmask + index_word) & (1 << index_bit);
    }

    void set_bit(bitmask_type* bitmask, size_type index, bool value) {
        auto index_word = word_index(index);
        auto index_bit = intra_word_index(index);
        auto& word = *(bitmask + index_word);
        if (value) {
            word |= (1 << index_bit);
        } else {
            word &= ~(1 << index_bit);
        }
    }

    bitmask_type funnel_shift_r(bitmask_type curr_word, bitmask_type next_word, size_type source_begin_bit) {
        bitmask_type mask = 0;
        bitmask_type result = 0;
        if (source_begin_bit != 0) {
            mask = set_least_significant_bits(source_begin_bit);
            result = (curr_word & mask) << (size_in_bits<bitmask_type>() - source_begin_bit);
        }
        if (source_begin_bit != size_in_bits<bitmask_type>()) {
            mask = set_least_significant_bits(size_in_bits<bitmask_type>() - source_begin_bit);
            result = result | (next_word & mask) >> source_begin_bit;
        }
        return result;
    }

    bitmask_type funnel_shift_l(bitmask_type curr_word, bitmask_type next_word, size_type source_end_bit) {
        bitmask_type mask = 0;
        bitmask_type result = 0;
        if (source_end_bit != 0) {
            mask = set_least_significant_bits(source_end_bit);
            result = (curr_word & mask) >> (size_in_bits<bitmask_type>() - source_end_bit);
        }
        if (source_end_bit != size_in_bits<bitmask_type>()) {
            mask = set_least_significant_bits(size_in_bits<bitmask_type>() - source_end_bit);
            result = result | (next_word & mask) << source_end_bit;
        }
        return result;
    }

} // namespace components::dataframe::detail