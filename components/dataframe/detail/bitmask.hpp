#pragma once

#include <vector>

#include <core/buffer.hpp>
#include <core/span.hpp>

#include <dataframe/enums.hpp>
#include <dataframe/forward.hpp>
#include <dataframe/types.hpp>

namespace components::dataframe::detail {

    core::buffer create_null_mask(std::pmr::memory_resource* resource, size_type size, mask_state state);

    size_type state_null_count(mask_state state, size_type size);

    std::size_t bitmask_allocation_size_bytes(size_type number_of_bits, std::size_t padding_boundary = 64);

    void set_null_mask(bitmask_type* bitmask, size_type begin_bit, size_type end_bit, bool valid);

    size_type
    count_set_bits(std::pmr::memory_resource* resource, bitmask_type const* bitmask, size_type start, size_type stop);

    size_type
    count_unset_bits(std::pmr::memory_resource* resource, bitmask_type const* bitmask, size_type start, size_type stop);

    std::pmr::vector<size_type> segmented_count_set_bits(std::pmr::memory_resource* resource,
                                                         bitmask_type const* bitmask,
                                                         core::span<size_type const> indices);

    std::pmr::vector<size_type> segmented_count_unset_bits(std::pmr::memory_resource* resource,
                                                           bitmask_type const* bitmask,
                                                           core::span<size_type const> indices);

    size_type
    null_count(std::pmr::memory_resource* resource, bitmask_type const* bitmask, size_type start, size_type stop);

    std::pmr::vector<size_type> segmented_valid_count(std::pmr::memory_resource* resource,
                                                      bitmask_type const* bitmask,
                                                      core::span<size_type const> indices);

    std::pmr::vector<size_type> segmented_null_count(std::pmr::memory_resource* resource,
                                                     bitmask_type const* bitmask,
                                                     core::span<size_type const> indices);

    core::buffer
    copy_bitmask(std::pmr::memory_resource* resource, bitmask_type const* mask, size_type begin_bit, size_type end_bit);

    core::buffer copy_bitmask(std::pmr::memory_resource* resource, column::column_view const& view);

    size_type num_bitmask_words(size_type number_of_bits);

    std::pmr::vector<size_type> segmented_null_count(std::pmr::memory_resource* resource,
                                                     const bitmask_type* bitmask,
                                                     core::span<const size_type> indices);

    class bitmask_iterator {
    public:
        using value_type = bool;
        using difference_type = size_type;
        using pointer = bool*;
        using reference = bool&;
        using iterator_category = std::random_access_iterator_tag;

        bitmask_iterator(const bitmask_type* data, size_type pos);
        explicit bitmask_iterator(const bitmask_type* data);
        bitmask_iterator() = delete;
        bitmask_iterator(const bitmask_iterator&) = default;
        bitmask_iterator& operator=(const bitmask_iterator&) = default;
        ~bitmask_iterator() = default;

        bool operator*() const;
        bitmask_iterator& operator++();
        bitmask_iterator operator++(int);
        bitmask_iterator& operator--();
        bitmask_iterator operator--(int);
        bool operator[](size_type n) const;
        bitmask_iterator& operator+=(size_type n);
        bitmask_iterator& operator-=(size_type n);

    private:
        const bitmask_type* data_{nullptr};
        size_type pos_{0};

        friend void swap(bitmask_iterator& a, bitmask_iterator& b);
        friend bool operator==(const bitmask_iterator& lhs, const bitmask_iterator& rhs);
        friend bool operator!=(const bitmask_iterator& lhs, const bitmask_iterator& rhs);
        friend bool operator<(const bitmask_iterator& lhs, const bitmask_iterator& rhs);
        friend bool operator>(const bitmask_iterator& lhs, const bitmask_iterator& rhs);
        friend bool operator<=(const bitmask_iterator& lhs, const bitmask_iterator& rhs);
        friend bool operator>=(const bitmask_iterator& lhs, const bitmask_iterator& rhs);
        friend size_type operator-(const bitmask_iterator& lhs, const bitmask_iterator& rhs);
    };

    class out_bitmask_iterator {
    public:
        using value_type = bool;
        using difference_type = size_type;
        using pointer = bool*;
        using reference = bool&;
        using iterator_category = std::output_iterator_tag;

        out_bitmask_iterator(bitmask_type* data, size_type pos);
        explicit out_bitmask_iterator(bitmask_type* data);
        out_bitmask_iterator() = delete;
        out_bitmask_iterator(const out_bitmask_iterator&) = default;
        out_bitmask_iterator& operator=(const out_bitmask_iterator&) = default;
        ~out_bitmask_iterator() = default;

        out_bitmask_iterator& operator*();
        out_bitmask_iterator& operator++();
        out_bitmask_iterator operator++(int);
        out_bitmask_iterator& operator=(bool value);

    private:
        bitmask_type* data_;
        size_type pos_;

        friend bool operator==(const out_bitmask_iterator& lhs, const out_bitmask_iterator& rhs);
        friend bool operator!=(const out_bitmask_iterator& lhs, const out_bitmask_iterator& rhs);
    };

} // namespace components::dataframe::detail
