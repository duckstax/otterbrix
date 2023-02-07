#include <memory>
#include <memory_resource>

#include <dataframe/column/make.hpp>
#include <dataframe/detail/bitmask.hpp>
#include <dataframe/detail/util.hpp>
#include <dataframe/dictionary/make_dictionary.hpp>
#include <dataframe/scalar/scalar.hpp>
#include <dataframe/traits.hpp>
#include <dataframe/type_dispatcher.hpp>
#include <dataframe/types.hpp>

namespace components::dataframe::column {
    namespace {
        struct size_of_helper {
            data_type type;
            template<typename T, std::enable_if_t<not is_fixed_width<T>()>* = nullptr>
            constexpr int operator()() const {
                assert(false);
                return 0;
            }

            template<typename T, std::enable_if_t<is_fixed_width<T>() && not is_fixed_point<T>()>* = nullptr>
            constexpr int operator()() const noexcept {
                return sizeof(T);
            }

            template<typename T, std::enable_if_t<is_fixed_point<T>()>* = nullptr>
            constexpr int operator()() const noexcept {
                return sizeof(typename T::rep);
            }
        };
    } // namespace

    std::size_t size_of(data_type element_type) {
        assertion_exception_msg(is_fixed_width(element_type), "Invalid element type.");
        return type_dispatcher(element_type, size_of_helper{element_type});
    }

    // Empty column of specified type
    std::unique_ptr<column_t> make_empty_column(std::pmr::memory_resource* resource, data_type type) {
        assertion_exception_msg(type.id() == type_id::empty || !is_nested(type), "make_empty_column is invalid to call on nested types");
        return std::make_unique<column_t>(resource, type, 0, core::buffer(resource), core::buffer(resource));
    }

    std::unique_ptr<column_t> make_empty_column(std::pmr::memory_resource* resource, type_id id) { return make_empty_column(resource, data_type{id}); }

    // Allocate storage for a specified number of numeric elements
    std::unique_ptr<column_t> make_numeric_column(
        std::pmr::memory_resource* resource,
        data_type type,
        size_type size,
        mask_state state) {
        assertion_exception_msg(is_numeric(type), "Invalid, non-numeric type.");
        assertion_exception_msg(size >= 0, "Column size cannot be negative.");

        return std::make_unique<column_t>(resource, type,
                                          size,
                                          core::buffer{resource, size * dataframe::size_of(type)},
                                          dataframe::detail::create_null_mask(resource, size, state),
                                          state_null_count(state, size),
                                          std::vector<std::unique_ptr<column_t>>{});
    }

    std::unique_ptr<column_t> make_fixed_point_column(
        std::pmr::memory_resource* resource,
        data_type type,
        size_type size,
        mask_state state) {
        assertion_exception_msg(is_fixed_point(type), "Invalid, non-fixed_point type.");
        assertion_exception_msg(size >= 0, "Column size cannot be negative.");

        return std::make_unique<column_t>(
            resource,
            type,
            size,
            core::buffer{resource, size * dataframe::size_of(type)},
            dataframe::detail::create_null_mask(resource, size, state),
            state_null_count(state, size),
            std::vector<std::unique_ptr<column_t>>{});
    }

    std::unique_ptr<column_t> make_timestamp_column(
        std::pmr::memory_resource* resource,
        data_type type,
        size_type size,
        mask_state state) {
        assertion_exception_msg(is_timestamp(type), "Invalid, non-timestamp type.");
        assertion_exception_msg(size >= 0, "Column size cannot be negative.");

        return std::make_unique<column_t>(
            resource,
            type,
            size,
            core::buffer{resource, size * dataframe::size_of(type)},
            dataframe::detail::create_null_mask(resource, size, state),
            state_null_count(state, size),
            std::vector<std::unique_ptr<column_t>>{});
    }

    std::unique_ptr<column_t> make_duration_column(
        std::pmr::memory_resource* resource, data_type type,
        size_type size,
        mask_state state) {
        assertion_exception_msg(is_duration(type), "Invalid, non-duration type.");
        assertion_exception_msg(size >= 0, "Column size cannot be negative.");

        return std::make_unique<column_t>(
            resource,
            type,
            size,
            core::buffer{resource, size * dataframe::size_of(type)},
            dataframe::detail::create_null_mask(resource, size, state),
            state_null_count(state, size),
            std::vector<std::unique_ptr<column_t>>{});
    }

    // Allocate storage for a specified number of fixed width elements
    std::unique_ptr<column_t> make_fixed_width_column(
        std::pmr::memory_resource* resource, data_type type,
        size_type size,
        mask_state state) {
        assertion_exception_msg(is_fixed_width(type), "Invalid, non-fixed-width type.");

        // clang-format off
  if      (is_timestamp  (type)) return make_timestamp_column  (resource,type, size, state);
  else if (is_duration   (type)) return make_duration_column   (resource,type, size, state);
  else if (is_fixed_point(type)) return make_fixed_point_column(resource,type, size, state);
  else                           return make_numeric_column    (resource,type, size, state);
        // clang-format on
    }

    std::unique_ptr<column_t> make_strings_column(
        std::pmr::memory_resource* resource,
        core::span<std::pair<const char*, size_type> const> strings) {
        //todo: impl
    }

    std::unique_ptr<column_t> make_strings_column(
        std::pmr::memory_resource* resource,
        core::span<std::string_view const> string_views,
        const std::string_view null_placeholder) {
        //todo: impl
    }

    std::unique_ptr<column_t> make_strings_column(
        std::pmr::memory_resource* resource,
        core::span<char const> strings,
        core::span<size_type const> offsets,
        core::span<bitmask_type const> null_mask,
        size_type null_count) {
        //todo: impl
    }

    std::unique_ptr<column_t> make_strings_column(std::pmr::memory_resource* resource,
                                                  size_type num_strings,
                                                  std::unique_ptr<column_t> offsets_column,
                                                  std::unique_ptr<column_t> chars_column,
                                                  size_type null_count,
                                                  core::buffer&& null_mask) {
        //todo: impl
    }

    std::unique_ptr<column_t> make_strings_column(std::pmr::memory_resource* resource,
                                                  size_type num_strings,
                                                  core::uvector<size_type>&& offsets,
                                                  core::uvector<char>&& chars,
                                                  core::buffer&& null_mask,
                                                  size_type null_count) {
        //todo: impl
    }

    std::unique_ptr<column_t> make_structs_column(
        std::pmr::memory_resource* resource,
        size_type num_rows,
        std::vector<std::unique_ptr<column_t>>&& child_columns,
        size_type null_count,
        core::buffer&& null_mask) {
        assertion_exception_msg(null_count <= 0 || !null_mask.is_empty(),
                                "Struct column with nulls must be nullable.");

        assertion_exception_msg(std::all_of(child_columns.begin(),
                                            child_columns.end(),
                                            [&](auto const& child_col) { return num_rows == child_col->size(); }),
                                "Child columns must have the same number of rows as the Struct column.");

        if (!null_mask.is_empty()) {
            for (auto& child : child_columns) {
                child = dataframe::detail::superimpose_nulls(resource,
                                                             static_cast<bitmask_type const*>(null_mask.data()),
                                                             null_count,
                                                             std::move(child));
            }
        }

        return std::make_unique<column::column_t>(resource,
                                                  data_type{type_id::structs},
                                                  num_rows,
                                                  core::buffer{resource},
                                                  std::move(null_mask),
                                                  null_count,
                                                  std::move(child_columns));
    }

    std::unique_ptr<column_t> make_column_from_scalar(
        std::pmr::memory_resource* resource,
        scalar::scalar_t const& s,
        size_type size) {
        //return type_dispatcher(s.type(), column_from_scalar_dispatch{}, s, size, stream, mr);
        //todo
    }

    std::unique_ptr<column_t> make_dictionary_from_scalar(std::pmr::memory_resource* resource,
                                                          scalar::scalar_t const& s,
                                                          size_type size) {
        if (size == 0) {
            return make_empty_column(resource, type_id::dictionary32);
        }
        assertion_exception_msg(size >= 0, "Column size cannot be negative.");
        return dictionary::make_dictionary_column(
            resource,
            make_column_from_scalar(resource, s, 1),
            make_column_from_scalar(resource, scalar::numeric_scalar<uint32_t>(resource, 0), size),
            core::buffer{resource, 0},
            0);
    }

} // namespace components::dataframe::column
