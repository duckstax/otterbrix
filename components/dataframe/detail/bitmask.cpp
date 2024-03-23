#include "bitmask.hpp"

#include <algorithm>
#include <memory>
#include <memory_resource>
#include <optional>
#include <vector>

#include <boost/iterator/counting_iterator.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/iterator/zip_iterator.hpp>

#include <core/buffer.hpp>
#include <core/popcount.hpp>
#include <core/scalar.hpp>
#include <core/span.hpp>
#include <core/uvector.hpp>

#include "dataframe/column/column_view.hpp"
#include "dataframe/math.hpp"
#include "dataframe/types.hpp"

#include "bits.hpp"

namespace components::dataframe::detail {

    std::size_t bitmask_allocation_size_bytes(size_type number_of_bits, std::size_t padding_boundary) {
        assertion_exception(padding_boundary > 0);
        auto necessary_bytes = math::div_rounding_up_safe<size_type>(number_of_bits, CHAR_BIT);
        auto padded_bytes =
            padding_boundary * math::div_rounding_up_safe<size_t>(size_t(necessary_bytes), padding_boundary);
        return padded_bytes;
    }

    size_type state_null_count(mask_state state, size_type size) {
        switch (state) {
            case mask_state::unallocated:
                return 0;
            case mask_state::uninitialized:
                return unknown_null_count;
            case mask_state::all_null:
                return size;
            case mask_state::all_valid:
                return 0;
            default:
                assert(false);
        }
    }

    core::buffer create_null_mask(std::pmr::memory_resource* mr, size_type size, mask_state state) {
        size_t mask_size{0};

        if (state != mask_state::unallocated) {
            mask_size = bitmask_allocation_size_bytes(size);
        }

        core::buffer mask(mr, mask_size);

        if (state != mask_state::uninitialized) {
            uint8_t fill_value = (state == mask_state::all_valid) ? 0xff : 0x00;
            std::memset(static_cast<bitmask_type*>(mask.data()), fill_value, mask_size);
        }

        return mask;
    }

    // TODO: Also make binops test that uses offset in column_view
    void copy_offset_bitmask(bitmask_type* destination,
                             bitmask_type const* source,
                             size_type source_begin_bit,
                             size_type source_end_bit,
                             size_type /*number_of_mask_words*/) {
        std::copy(bitmask_iterator(source, source_begin_bit),
                  bitmask_iterator(source, source_end_bit),
                  out_bitmask_iterator(destination));
    }

    enum class count_bits_policy : bool
    {
        unset_bits, /// Count unset (0) bits
        set_bits    /// Count set (1) bits
    };

    template<typename OffsetIterator, typename OutputIterator>
    void subtract_set_bits_range_boundaries_kernel(bitmask_type const* bitmask,
                                                   size_type num_ranges,
                                                   OffsetIterator first_bit_indices,
                                                   OffsetIterator last_bit_indices,
                                                   OutputIterator null_counts) {
        constexpr size_type const word_size_in_bits{detail::size_in_bits<bitmask_type>()};

        size_type range_id = 0;

        while (range_id < num_ranges) {
            size_type const first_bit_index = *(first_bit_indices + range_id);
            size_type const last_bit_index = *(last_bit_indices + range_id);
            size_type delta = 0;

            // Compute delta due to the preceding bits in the first word in the range.
            size_type const first_num_slack_bits = intra_word_index(first_bit_index);
            if (first_num_slack_bits > 0) {
                bitmask_type const word = bitmask[word_index(first_bit_index)];
                bitmask_type const slack_mask = set_least_significant_bits(first_num_slack_bits);
                delta -= core::popcount(word & slack_mask);
            }

            // Compute delta due to the following bits in the last word in the range.
            size_type const last_num_slack_bits =
                (last_bit_index % word_size_in_bits) == 0 ? 0 : word_size_in_bits - intra_word_index(last_bit_index);
            if (last_num_slack_bits > 0) {
                bitmask_type const word = bitmask[word_index(last_bit_index)];
                bitmask_type const slack_mask = set_most_significant_bits(last_num_slack_bits);
                delta -= core::popcount(word & slack_mask);
            }

            // Update the null count with the computed delta.
            size_type updated_null_count = *(null_counts + range_id) + delta;
            *(null_counts + range_id) = updated_null_count;
            range_id += 1;
        }
    }

    struct bit_to_word_index final {
        bit_to_word_index(bool inclusive)
            : inclusive(inclusive) {}
        inline size_type operator()(const size_type& bit_index) const {
            return word_index(bit_index) + ((inclusive || intra_word_index(bit_index) == 0) ? 0 : 1);
        }
        bool const inclusive;
    };

    template<typename InputIteratorT,
             typename OutputIteratorT,
             typename BeginOffsetIteratorT,
             typename EndOffsetIteratorT>
    void Sum(void* d_temp_storage,
             size_t& temp_storage_bytes,
             InputIteratorT d_in,
             OutputIteratorT d_out,
             int num_segments,
             BeginOffsetIteratorT d_begin_offsets,
             EndOffsetIteratorT d_end_offsets) {
        // Calculate the size of an element in the input range
        using value_type = typename std::iterator_traits<InputIteratorT>::value_type;
        size_t element_size = sizeof(value_type);

        // Calculate the required allocation size for the temporary storage buffer
        //!size_t num_elements = std::distance(d_in, d_out);
        temp_storage_bytes = element_size * static_cast<size_t>(num_segments);

        // If d_temp_storage is NULL, return the required allocation size
        if (!d_temp_storage) {
            return;
        }

        for (auto it_begin = d_begin_offsets, it_end = d_end_offsets, end = d_begin_offsets + num_segments;
             it_begin != end;
             ++it_begin, ++it_end) {
            *d_out++ = std::accumulate(d_in + *it_begin, d_in + *it_end, 0);
        }
    }

    template<typename OffsetIterator>
    core::uvector<size_type> segmented_count_bits(std::pmr::memory_resource* resource,
                                                  bitmask_type const* bitmask,
                                                  OffsetIterator first_bit_indices_begin,
                                                  OffsetIterator first_bit_indices_end,
                                                  OffsetIterator last_bit_indices_begin,
                                                  count_bits_policy count_bit) {
        auto const num_ranges = static_cast<size_type>(std::distance(first_bit_indices_begin, first_bit_indices_end));
        core::uvector<size_type> d_bit_counts(resource, size_t(num_ranges));

        auto popc = [](bitmask_type word) -> size_type { return static_cast<size_type>(core::popcount(word)); };

        auto num_set_bits_in_word = boost::iterators::make_transform_iterator(bitmask, popc);
        auto first_word_indices =
            boost::iterators::make_transform_iterator(first_bit_indices_begin, bit_to_word_index{true});
        auto last_word_indices =
            boost::iterators::make_transform_iterator(last_bit_indices_begin, bit_to_word_index{false});

        // Allocate temporary memory.
        size_t temp_storage_bytes{0};
        Sum(nullptr,
            temp_storage_bytes,
            num_set_bits_in_word,
            d_bit_counts.begin(),
            num_ranges,
            first_word_indices,
            last_word_indices);

        core::buffer d_temp_storage(resource, temp_storage_bytes);

        // Perform segmented reduction.
        Sum(d_temp_storage.data(),
            temp_storage_bytes,
            num_set_bits_in_word,
            d_bit_counts.begin(),
            num_ranges,
            first_word_indices,
            last_word_indices);

        // Adjust counts in segment boundaries (if segments are not word-aligned).
        //constexpr size_type block_size{256};
        subtract_set_bits_range_boundaries_kernel(bitmask,
                                                  num_ranges,
                                                  first_bit_indices_begin,
                                                  last_bit_indices_begin,
                                                  d_bit_counts.begin());

        if (count_bit == count_bits_policy::unset_bits) {
            // Convert from set bits counts to unset bits by subtracting the number of
            // set bits from the length of the segment.
            auto segments_begin =
                boost::iterators::make_zip_iterator(boost::make_tuple(first_bit_indices_begin, last_bit_indices_begin));
            auto segment_length_iterator =
                boost::iterators::transform_iterator(segments_begin, [](auto const& segment) {
                    auto const begin = boost::get<0>(segment);
                    auto const end = boost::get<1>(segment);
                    return end - begin;
                });
            std::transform(segment_length_iterator,
                           segment_length_iterator + num_ranges,
                           d_bit_counts.data(),
                           d_bit_counts.data(),
                           [](auto segment_size, auto segment_bit_count) { return segment_size - segment_bit_count; });
        }

        return d_bit_counts;
    }

    template<typename IndexIterator>
    size_type validate_segmented_indices(IndexIterator indices_begin, IndexIterator indices_end) {
        auto const num_indices = static_cast<size_type>(std::distance(indices_begin, indices_end));
        assertion_exception_msg(num_indices % 2 == 0, "Array of indices needs to have an even number of elements.");
        size_type const num_segments = num_indices / 2;
        for (size_type i = 0; i < num_segments; i++) {
            auto begin = indices_begin[2 * i];
            auto end = indices_begin[2 * i + 1];
            assertion_exception_msg(begin >= 0, "Starting index cannot be negative.");
            assertion_exception_msg(end >= begin, "End index cannot be smaller than the starting index.");
        }
        return num_segments;
    }

    struct index_t final {
        inline size_type operator()(const size_type& i) const { return *(d_indices + 2 * i + (is_end ? 1 : 0)); }

        bool const is_end = false;
        const size_type* d_indices;
    };

    template<typename IndexIterator>
    std::pmr::vector<size_type> segmented_count_bits(std::pmr::memory_resource* resource,
                                                     bitmask_type const* bitmask,
                                                     IndexIterator indices_begin,
                                                     IndexIterator indices_end,
                                                     count_bits_policy count_bits) {
        assertion_exception_msg(bitmask != nullptr, "Invalid bitmask.");
        auto const num_segments = validate_segmented_indices(indices_begin, indices_end);

        // Return an empty vector if there are zero segments.
        if (num_segments == 0) {
            return std::pmr::vector<size_type>{};
        }

        // Construct a contiguous host buffer of indices and copy to device.
        auto const d_indices = core::make_uvector<size_type>(resource, indices_begin, indices_end);

        // Compute the bit counts over each segment.
        auto first_bit_indices_begin =
            boost::iterators::make_transform_iterator(boost::iterators::make_counting_iterator(0),
                                                      index_t{false, d_indices.data()});
        auto const first_bit_indices_end = first_bit_indices_begin + num_segments;
        auto last_bit_indices_begin =
            boost::iterators::make_transform_iterator(boost::iterators::make_counting_iterator(0),
                                                      index_t{true, d_indices.data()});
        core::uvector<size_type> d_bit_counts = segmented_count_bits(resource,
                                                                     bitmask,
                                                                     first_bit_indices_begin,
                                                                     first_bit_indices_end,
                                                                     last_bit_indices_begin,
                                                                     count_bits);
        return core::make_vector(resource, d_bit_counts);
    }

    // Count non-zero bits in the specified ranges.
    template<typename IndexIterator>
    std::pmr::vector<size_type> segmented_count_set_bits(std::pmr::memory_resource* resource,
                                                         bitmask_type const* bitmask,
                                                         IndexIterator indices_begin,
                                                         IndexIterator indices_end) {
        return segmented_count_bits(resource, bitmask, indices_begin, indices_end, count_bits_policy::set_bits);
    }

    std::pmr::vector<size_type> segmented_count_set_bits(std::pmr::memory_resource* resource,
                                                         bitmask_type const* bitmask,
                                                         core::span<size_type const> indices) {
        return segmented_count_set_bits(resource, bitmask, indices.begin(), indices.end());
    }

    // Count zero bits in the specified ranges.
    template<typename IndexIterator>
    std::pmr::vector<size_type> segmented_count_unset_bits(std::pmr::memory_resource* resource,
                                                           bitmask_type const* bitmask,
                                                           IndexIterator indices_begin,
                                                           IndexIterator indices_end) {
        return segmented_count_bits(resource, bitmask, indices_begin, indices_end, count_bits_policy::unset_bits);
    }

    std::pmr::vector<size_type> segmented_count_unset_bits(std::pmr::memory_resource* resource,
                                                           bitmask_type const* bitmask,
                                                           core::span<size_type const> indices) {
        return segmented_count_unset_bits(resource, bitmask, indices.begin(), indices.end());
    }

    // Count valid elements in the specified ranges of a validity bitmask.
    template<typename IndexIterator>
    std::pmr::vector<size_type> segmented_valid_count(std::pmr::memory_resource* resource,
                                                      bitmask_type const* bitmask,
                                                      IndexIterator indices_begin,
                                                      IndexIterator indices_end) {
        if (bitmask == nullptr) {
            // Return a vector of segment lengths.
            auto const num_segments = size_t(validate_segmented_indices(indices_begin, indices_end));
            auto ret = std::pmr::vector<size_type>(num_segments, 0);
            for (size_t i = 0; i < num_segments; i++) {
                ret[i] = indices_begin[2 * i + 1] - indices_begin[2 * i];
            }
            return ret;
        }

        return segmented_count_set_bits(resource, bitmask, indices_begin, indices_end);
    }

    std::pmr::vector<size_type> segmented_valid_count(std::pmr::memory_resource* resource,
                                                      const bitmask_type* bitmask,
                                                      core::span<const size_type> indices) {
        return segmented_valid_count(resource, bitmask, indices.begin(), indices.end());
    }

    // Count null elements in the specified ranges of a validity bitmask.
    template<typename IndexIterator>
    std::pmr::vector<size_type> segmented_null_count(std::pmr::memory_resource* resource,
                                                     bitmask_type const* bitmask,
                                                     IndexIterator indices_begin,
                                                     IndexIterator indices_end) {
        if (bitmask == nullptr) {
            // Return a vector of zeros.
            auto const num_segments = validate_segmented_indices(indices_begin, indices_end);
            return std::pmr::vector<size_type>(num_segments, 0);
        }
        return detail::segmented_count_unset_bits(resource, bitmask, indices_begin, indices_end);
    }

    core::buffer copy_bitmask(std::pmr::memory_resource* resource,
                              bitmask_type const* mask,
                              size_type begin_bit,
                              size_type end_bit) {
        assertion_exception(begin_bit >= 0);
        assertion_exception(begin_bit <= end_bit);
        core::buffer dest_mask{resource};
        auto num_bytes = bitmask_allocation_size_bytes(end_bit - begin_bit);
        if ((mask == nullptr) || (num_bytes == 0)) {
            return dest_mask;
        }
        if (begin_bit == 0) {
            dest_mask = core::buffer{resource, static_cast<void const*>(mask), num_bytes};
        } else {
            auto number_of_mask_words = num_bitmask_words(end_bit - begin_bit);
            dest_mask = core::buffer{resource, num_bytes};
            copy_offset_bitmask(static_cast<bitmask_type*>(dest_mask.data()),
                                mask,
                                begin_bit,
                                end_bit,
                                number_of_mask_words);
        }
        return dest_mask;
    }

    core::buffer copy_bitmask(std::pmr::memory_resource* mr, column::column_view const& view) {
        core::buffer null_mask{mr, 0};
        if (view.nullable()) {
            null_mask = copy_bitmask(mr, view.null_mask(), view.offset(), view.offset() + view.size());
        }
        return null_mask;
    }

    void set_null_mask(bitmask_type* bitmask, size_type begin_bit, size_type end_bit, bool valid) {
        assertion_exception(begin_bit >= 0);
        assertion_exception(begin_bit <= end_bit);
        if (begin_bit == end_bit)
            return;
        if (bitmask != nullptr) {
            std::fill(out_bitmask_iterator{bitmask, begin_bit}, out_bitmask_iterator{bitmask, end_bit}, valid);
        }
    }

    // Count non-zero bits in the specified range
    size_type count_set_bits(std::pmr::memory_resource* /*resource*/,
                             bitmask_type const* bitmask,
                             size_type start,
                             size_type stop) {
        assertion_exception_msg(bitmask != nullptr, "Invalid bitmask.");
        assertion_exception_msg(start >= 0, "Invalid range.");
        assertion_exception_msg(start <= stop, "Invalid bit range.");

        auto const num_bits_to_count = stop - start;
        if (num_bits_to_count == 0) {
            return 0;
        }

        return size_type(std::count_if(bitmask_iterator{bitmask, start},
                                       bitmask_iterator{bitmask, stop},
                                       [](bool value) { return value; }));
    }

    // Count zero bits in the specified range
    size_type count_unset_bits(std::pmr::memory_resource* resource,
                               bitmask_type const* bitmask,
                               size_type start,
                               size_type stop) {
        auto const num_set_bits = count_set_bits(resource, bitmask, start, stop);
        auto const total_num_bits = (stop - start);
        return total_num_bits - num_set_bits;
    }

    // Count null elements in the specified range of a validity bitmask
    size_type
    null_count(std::pmr::memory_resource* resource, bitmask_type const* bitmask, size_type start, size_type stop) {
        if (bitmask == nullptr) {
            assertion_exception_msg(start >= 0, "Invalid range.");
            assertion_exception_msg(start <= stop, "Invalid bit range.");
            return 0;
        }

        return count_unset_bits(resource, bitmask, start, stop);
    }

    size_type num_bitmask_words(size_type number_of_bits) {
        return math::div_rounding_up_safe<size_type>(number_of_bits, detail::size_in_bits<bitmask_type>());
    }

    std::pmr::vector<size_type> segmented_null_count(std::pmr::memory_resource* resource,
                                                     const bitmask_type* bitmask,
                                                     core::span<const size_type> indices) {
        return segmented_null_count(resource, bitmask, indices.begin(), indices.end());
    }

    bitmask_iterator::bitmask_iterator(const bitmask_type* data, size_type pos)
        : data_(data)
        , pos_(pos) {}

    bitmask_iterator::bitmask_iterator(const bitmask_type* data)
        : bitmask_iterator(data, 0) {}

    bool bitmask_iterator::operator*() const { return is_set_bit(data_, pos_); }

    bitmask_iterator& bitmask_iterator::operator++() {
        ++pos_;
        return *this;
    }

    bitmask_iterator bitmask_iterator::operator++(int) {
        auto old = *this;
        ++(*this);
        return old;
    }

    bitmask_iterator& bitmask_iterator::operator--() {
        --pos_;
        return *this;
    }

    bitmask_iterator bitmask_iterator::operator--(int) {
        auto old = *this;
        --(*this);
        return old;
    }

    bool bitmask_iterator::operator[](size_type n) const { return is_set_bit(data_, n); }

    bitmask_iterator& bitmask_iterator::operator+=(size_type n) {
        pos_ += n;
        return *this;
    }

    bitmask_iterator& bitmask_iterator::operator-=(size_type n) {
        pos_ -= n;
        return *this;
    }

    void swap(bitmask_iterator& a, bitmask_iterator& b) { std::swap(a.pos_, b.pos_); }

    bool operator==(const bitmask_iterator& lhs, const bitmask_iterator& rhs) { return lhs.pos_ == rhs.pos_; }

    bool operator!=(const bitmask_iterator& lhs, const bitmask_iterator& rhs) { return !(lhs == rhs); }

    bool operator<(const bitmask_iterator& lhs, const bitmask_iterator& rhs) { return lhs.pos_ < rhs.pos_; }

    bool operator>(const bitmask_iterator& lhs, const bitmask_iterator& rhs) { return rhs < lhs; }

    bool operator<=(const bitmask_iterator& lhs, const bitmask_iterator& rhs) { return !(rhs > lhs); }

    bool operator>=(const bitmask_iterator& lhs, const bitmask_iterator& rhs) { return !(lhs < rhs); }

    size_type operator-(const bitmask_iterator& lhs, const bitmask_iterator& rhs) { return lhs.pos_ - rhs.pos_; }

    out_bitmask_iterator::out_bitmask_iterator(bitmask_type* data, size_type pos)
        : data_(data)
        , pos_(pos) {}

    out_bitmask_iterator::out_bitmask_iterator(bitmask_type* data)
        : out_bitmask_iterator(data, 0) {}

    out_bitmask_iterator& out_bitmask_iterator::operator*() { return *this; }

    out_bitmask_iterator& out_bitmask_iterator::operator++() {
        ++pos_;
        return *this;
    }

    out_bitmask_iterator out_bitmask_iterator::operator++(int) {
        auto old = *this;
        ++(*this);
        return old;
    }

    out_bitmask_iterator& out_bitmask_iterator::operator=(bool value) {
        set_bit(data_, pos_, value);
        return *this;
    }

    bool operator==(const out_bitmask_iterator& lhs, const out_bitmask_iterator& rhs) { return lhs.pos_ == rhs.pos_; }

    bool operator!=(const out_bitmask_iterator& lhs, const out_bitmask_iterator& rhs) { return !(lhs == rhs); }

} // namespace components::dataframe::detail
