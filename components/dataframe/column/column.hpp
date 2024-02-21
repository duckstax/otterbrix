#pragma once

#include <cassert>

#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "column_view.hpp"
#include "dataframe/bitmask.hpp"
#include "dataframe/types.hpp"
#include <dataframe/traits.hpp>

#include "core/buffer.hpp"
#include "core/uvector.hpp"

namespace components::dataframe::column {

    class column_t {
    public:
        column_t() = default;
        ~column_t() = default;
        column_t& operator=(column_t const& other) = delete;
        column_t& operator=(column_t&& other) = delete;
        column_t(std::pmr::memory_resource*, column_t const& other);
        column_t(column_t&& other) noexcept;

        template<typename T>
        column_t(std::pmr::memory_resource* resource,
                 core::uvector<T>&& other,
                 core::buffer&& null_mask,
                 size_type null_count = unknown_null_count)
            : type_(data_type(type_to_id<T>()))
            , size_{[&]() {
                assert(other.size() <= static_cast<std::size_t>(std::numeric_limits<size_type>::max()));
                return static_cast<size_type>(other.size());
            }()}
            , resource_(resource)
            , data_{other.release()}
            , null_mask_{std::move(null_mask)}
            , null_count_{null_count} {}

        template<typename B1, typename B2 = core::buffer>
        column_t(std::pmr::memory_resource* resource,
                 data_type dtype,
                 size_type size,
                 B1&& data,
                 B2&& null_mask,
                 size_type null_count = unknown_null_count,
                 std::vector<std::unique_ptr<column_t>>&& children = {})
            : type_{dtype}
            , size_{[&]() {
                assert(size >= 0);
                return size;
            }()}
            , resource_(resource)
            , data_{std::forward<B1>(data)}
            , null_mask_{std::forward<B2>(null_mask)}
            , null_count_{null_count}
            , children_{std::move(children)} {}

        explicit column_t(std::pmr::memory_resource*, column_view view);
        [[nodiscard]] data_type type() const noexcept;
        [[nodiscard]] size_type size() const noexcept;
        [[nodiscard]] size_type null_count() const;
        void set_null_mask(core::buffer&& new_null_mask, size_type new_null_count = unknown_null_count);
        void set_null_mask(core::buffer const& new_null_mask, size_type new_null_count = unknown_null_count);
        void set_null_count(size_type new_null_count);
        [[nodiscard]] bool nullable() const noexcept;
        [[nodiscard]] bool has_nulls() const noexcept;
        [[nodiscard]] size_type num_children() const noexcept;
        column_t& child(size_type child_index) noexcept;
        [[nodiscard]] column_t const& child(size_type child_index) const noexcept;

        struct contents final {
            std::unique_ptr<core::buffer> data;
            std::unique_ptr<core::buffer> null_mask;
            std::vector<std::unique_ptr<column_t>> children;
        };

        contents release() noexcept;
        [[nodiscard]] column_view view() const;
        operator column_view() const;
        mutable_column_view mutable_view();
        operator mutable_column_view();

    private:
        data_type type_{type_id::empty};
        size_type size_{};
        std::pmr::memory_resource* resource_;
        core::buffer data_;
        core::buffer null_mask_;
        mutable size_type null_count_{unknown_null_count};
        std::vector<std::unique_ptr<column_t>> children_;
    };

} // namespace components::dataframe::column
