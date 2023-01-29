#pragma once

#include <cassert>

#include <memory>
#include <memory_resource>
#include <type_traits>
#include <utility>
#include <vector>

#include "column.hpp"
#include "column_view.hpp"
#include "dataframe/traits.hpp"
#include "dataframe/types.hpp"

#include "core/buffer.hpp"
#include "core/uvector.hpp"

namespace components::dataframe::column {

    std::unique_ptr<column_t> make_empty_column(std::pmr::memory_resource* resource, data_type type);
    std::unique_ptr<column_t> make_empty_column(std::pmr::memory_resource* resource, type_id id);

    std::unique_ptr<column_t> make_numeric_column(
        std::pmr::memory_resource* resource,
        data_type type,
        size_type size,
        mask_state state = mask_state::unallocated);

    template<typename B>
    std::unique_ptr<column_t> make_numeric_column(
        std::pmr::memory_resource* resource,
        data_type type,
        size_type size,
        B&& null_mask,
        size_type null_count = dataframe::unknown_null_count) {
        assertion_exception_msg(is_numeric(type), "Invalid, non-numeric type.");
        return std::make_unique<column_t>(type,
                                          size,
                                          core::buffer{resource, size * dataframe::size_of(type)},
                                          std::forward<B>(null_mask),
                                          null_count);
    }

    std::unique_ptr<column_t> make_fixed_point_column(
        std::pmr::memory_resource* resource,
        data_type type,
        size_type size,
        mask_state state = mask_state::unallocated);

    template<typename B>
    std::unique_ptr<column_t> make_fixed_point_column(
        std::pmr::memory_resource* resource,
        data_type type,
        size_type size,
        B&& null_mask,
        size_type null_count = dataframe::unknown_null_count) {
        assertion_exception_msg(is_fixed_point(type), "Invalid, non-fixed_point type.");
        return std::make_unique<column_t>(type,
                                          size,
                                          core::buffer{resource, size * dataframe::size_of(type)},
                                          std::forward<B>(null_mask),
                                          null_count);
    }

    std::unique_ptr<column_t> make_timestamp_column(
        std::pmr::memory_resource* resource,
        data_type type,
        size_type size,
        mask_state state = mask_state::unallocated);

    template<typename B>
    std::unique_ptr<column_t> make_timestamp_column(
        std::pmr::memory_resource* resource,
        data_type type,
        size_type size,
        B&& null_mask,
        size_type null_count = dataframe::unknown_null_count) {
        assertion_exception_msg(is_timestamp(type), "Invalid, non-timestamp type.");
        return std::make_unique<column_t>(type,
                                          size,
                                          core::buffer{resource, size * dataframe::size_of(type)},
                                          std::forward<B>(null_mask),
                                          null_count);
    }

    std::unique_ptr<column_t> make_duration_column(
        std::pmr::memory_resource* resource,
        data_type type,
        size_type size,
        mask_state state = mask_state::unallocated);

    template<typename B>
    std::unique_ptr<column_t> make_duration_column(
        std::pmr::memory_resource* resource,
        data_type type,
        size_type size,
        B&& null_mask,
        size_type null_count = dataframe::unknown_null_count) {
        assertion_exception_msg(is_duration(type), "Invalid, non-duration type.");
        return std::make_unique<column_t>(type,
                                          size,
                                          core::buffer{resource, size * dataframe::size_of(type)},
                                          std::forward<B>(null_mask),
                                          null_count);
    }

    std::unique_ptr<column_t> make_fixed_width_column(
        std::pmr::memory_resource* resource,
        data_type type,
        size_type size,
        mask_state state = mask_state::unallocated);

    template<typename B>
    std::unique_ptr<column_t> make_fixed_width_column(
        std::pmr::memory_resource* resource,
        data_type type,
        size_type size,
        B&& null_mask,
        size_type null_count = dataframe::unknown_null_count) {
        assertion_exception_msg(is_fixed_width(type), "Invalid, non-fixed-width type.");
        if (is_timestamp(type)) {
            return make_timestamp_column(resource, type, size, std::forward<B>(null_mask), null_count);
        } else if (is_duration(type)) {
            return make_duration_column(resource, type, size, std::forward<B>(null_mask), null_count);
        } else if (is_fixed_point(type)) {
            return make_fixed_point_column(resource, type, size, std::forward<B>(null_mask), null_count);
        }
        return make_numeric_column(type, size, std::forward<B>(null_mask), null_count);
    }

    std::unique_ptr<column_t> make_strings_column(std::pmr::memory_resource* resource, core::span<std::pair<const char*, size_type> const> strings);

    std::unique_ptr<column_t> make_strings_column(
        std::pmr::memory_resource* resource,
        core::span<std::string_view const> string_views,
        const std::string_view null_placeholder);

    std::unique_ptr<column_t> make_strings_column(
        std::pmr::memory_resource* resource,
        core::span<char const> strings,
        core::span<size_type const> offsets,
        core::span<bitmask_type const> null_mask = {},
        size_type null_count = dataframe::unknown_null_count);

    std::unique_ptr<column_t> make_strings_column(std::pmr::memory_resource* resource,
                                                  size_type num_strings,
                                                  std::unique_ptr<column_t> offsets_column,
                                                  std::unique_ptr<column_t> chars_column,
                                                  size_type null_count,
                                                  core::buffer&& null_mask);

    std::unique_ptr<column_t> make_strings_column(std::pmr::memory_resource* resource,
                                                  size_type num_strings,
                                                  core::uvector<size_type>&& offsets,
                                                  core::uvector<char>&& chars,
                                                  core::buffer&& null_mask,
                                                  size_type null_count = dataframe::unknown_null_count);


    std::unique_ptr<column_t> make_structs_column(
        std::pmr::memory_resource* resource,
        size_type num_rows,
        std::vector<std::unique_ptr<column_t>>&& child_columns,
        size_type null_count,
        core::buffer&& null_mask);

    std::unique_ptr<column_t> make_column_from_scalar(
        std::pmr::memory_resource* resource,
        scalar::scalar_t const& s,
        size_type size);

    std::unique_ptr<column_t> make_dictionary_from_scalar(
        std::pmr::memory_resource* resource,
        scalar::scalar_t const& s,
        size_type size);

} // namespace components::dataframe::column
