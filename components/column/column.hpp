#pragma once

#include <cassert>

#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "column_view.hpp"
#include "null_mask.hpp"
#include "types.hpp"

#include <core/buffer.hpp>
#include <core/uvector.hpp>

namespace components::column {

    class column {
    public:
        column() = default;
        ~column() = default;
        column& operator=(column const& other) = delete;
        column& operator=(column&& other) = delete;
        column(column const& other, std::pmr::memory_resource*);
        column(column&& other) noexcept;

        template<typename T>
        column(core::uvector<T>&& other,
               core::buffer&& null_mask = {},
               size_type null_count = UNKNOWN_NULL_COUNT)
            : _type{data_type{type_to_id<T>()}}
            , _size{[&]() {
                assert(other.size() <= static_cast<std::size_t>(std::numeric_limits<size_type>::max()));
                return static_cast<size_type>(other.size());
            }()}
            , _data{other.release()}
            , _null_mask{std::move(null_mask)}
            , _null_count{null_count} {
        }

        template<typename B1, typename B2 = core::buffer>
        column(data_type dtype,
               size_type size,
               B1&& data,
               B2&& null_mask = {},
               size_type null_count = UNKNOWN_NULL_COUNT,
               std::vector<std::unique_ptr<column>>&& children = {})
            : _type{dtype}
            , _size{size}
            , _data{std::forward<B1>(data)}
            , _null_mask{std::forward<B2>(null_mask)}
            , _null_count{null_count}
            , _children{std::move(children)} {
            assert(size >= 0);
        }

        explicit column(column_view view, std::pmr::memory_resource*);
        [[nodiscard]] data_type type() const noexcept;
        [[nodiscard]] size_type size() const noexcept;
        [[nodiscard]] size_type null_count() const;
        void set_null_mask(core::buffer&& new_null_mask, size_type new_null_count = UNKNOWN_NULL_COUNT);
        void set_null_mask(core::buffer const& new_null_mask, size_type new_null_count = UNKNOWN_NULL_COUNT);
        void set_null_count(size_type new_null_count);
        [[nodiscard]] bool nullable() const noexcept;
        [[nodiscard]] bool has_nulls() const noexcept;
        [[nodiscard]] size_type num_children() const noexcept;
        column& child(size_type child_index) noexcept;
        [[nodiscard]] column const& child(size_type child_index) const noexcept;

        struct contents final {
            std::unique_ptr<core::buffer> data;
            std::unique_ptr<core::buffer> null_mask;
            std::vector<std::unique_ptr<column>> children;
        };

        contents release() noexcept;
        [[nodiscard]] column_view view() const;
        operator column_view() const;
        mutable_column_view mutable_view();
        operator mutable_column_view();

    private:
        data_type _type{type_id::EMPTY};
        size_type _size{};
        core::buffer _data{};
        core::buffer _null_mask{};
        mutable size_type _null_count{UNKNOWN_NULL_COUNT};
        std::vector<std::unique_ptr<column>> _children{};
    };

} // namespace components::column
