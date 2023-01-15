#pragma once

#include <vector>

#include <core/buffer.hpp>
#include <core/span.hpp>

#include <dataframe/types.hpp>
#include <dataframe/enums.hpp>

namespace components::dataframe::detail {

    core::buffer create_null_mask(
        std::pmr::memory_resource* resource,
        size_type size,
        mask_state state);

    size_type state_null_count(
        mask_state state,
        size_type size);

    std::size_t bitmask_allocation_size_bytes(
        size_type number_of_bits,
        std::size_t padding_boundary = 64);

    void set_null_mask(
        bitmask_type* bitmask,
        size_type begin_bit,
        size_type end_bit,
        bool valid);

    size_type count_set_bits(
        std::pmr::memory_resource* resource,
        bitmask_type const* bitmask,
        size_type start,
        size_type stop);

    size_type count_unset_bits(
        std::pmr::memory_resource* resource,
        bitmask_type const* bitmask,
        size_type start,
        size_type stop);

    std::pmr::vector<size_type> segmented_count_set_bits(
        std::pmr::memory_resource* resource,
        bitmask_type const* bitmask,
        core::span<size_type const> indices);

    std::pmr::vector<size_type> segmented_count_unset_bits(
        std::pmr::memory_resource* resource,
        bitmask_type const* bitmask,
        core::span<size_type const> indices);

    size_type null_count(
        std::pmr::memory_resource* resource,
        bitmask_type const* bitmask,
        size_type start,
        size_type stop);

    std::pmr::vector<size_type> segmented_valid_count(
        std::pmr::memory_resource* resource,
        bitmask_type const* bitmask,
        core::span<size_type const> indices);

    std::pmr::vector<size_type> segmented_null_count(
        std::pmr::memory_resource* resource,
        bitmask_type const* bitmask,
        core::span<size_type const> indices);

    core::buffer copy_bitmask(
        std::pmr::memory_resource* resource,
        bitmask_type const* mask,
        size_type begin_bit,
        size_type end_bit);

    size_type num_bitmask_words(size_type number_of_bits);

} // namespace components::dataframe::detail
