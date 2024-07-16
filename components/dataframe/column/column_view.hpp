#pragma once

#include <cstdint>

#include <limits>
#include <type_traits>
#include <vector>

#include "core/assert/assert.hpp"
#include "core/span.hpp"
#include <dataframe/traits.hpp>
#include <dataframe/type_dispatcher.hpp>
#include <dataframe/types.hpp>

namespace components::dataframe::column {
    namespace detail {
        class column_view_base {
        public:
            template<typename R = void,
                     typename = std::enable_if_t<std::is_same_v<R, void> or is_rep_layout_compatible<R>()>>
            R const* head() const noexcept {
                return static_cast<R const*>(_data);
            }

            template<typename T, typename = std::enable_if_t<is_rep_layout_compatible<T>()>>
            T const* data() const noexcept {
                return head<T>() + _offset;
            }

            template<typename T, typename = std::enable_if_t<is_rep_layout_compatible<T>()>>
            T const* begin() const noexcept {
                return data<T>();
            }

            template<typename T, typename = std::enable_if_t<is_rep_layout_compatible<T>()>>
            T const* end() const noexcept {
                return begin<T>() + size();
            }

            [[nodiscard]] size_type size() const noexcept { return _size; }
            [[nodiscard]] bool is_empty() const noexcept { return size() == 0; }
            [[nodiscard]] data_type type() const noexcept { return _type; }
            [[nodiscard]] bool nullable() const noexcept { return nullptr != _null_mask; }
            [[nodiscard]] size_type null_count(std::pmr::memory_resource* resource) const;
            [[nodiscard]] size_type
            null_count(size_type begin, size_type end, std::pmr::memory_resource* resource) const;
            [[nodiscard]] bool has_nulls(std::pmr::memory_resource* resource) const { return null_count(resource) > 0; }
            [[nodiscard]] bool has_nulls(size_type begin, size_type end, std::pmr::memory_resource* resource) const {
                return null_count(begin, end, resource) > 0;
            }

            [[nodiscard]] bitmask_type const* null_mask() const noexcept { return _null_mask; }
            [[nodiscard]] size_type offset() const noexcept { return _offset; }

        protected:
            data_type _type{type_id::empty};
            size_type _size{};
            void const* _data{};
            bitmask_type const* _null_mask{};
            mutable size_type _null_count{};
            size_type _offset{};

            column_view_base() = default;
            ~column_view_base() = default;
            column_view_base(column_view_base const&) = default;
            column_view_base(column_view_base&&) = default;

            column_view_base& operator=(column_view_base const&) = default;
            column_view_base& operator=(column_view_base&&) = default;

            column_view_base(data_type type,
                             size_type size,
                             void const* data,
                             bitmask_type const* null_mask = nullptr,
                             size_type null_count = unknown_null_count,
                             size_type offset = 0);
        };

        class mutable_column_view_base : public column_view_base {};

    } // namespace detail

    class column_view : public detail::column_view_base {
    public:
        column_view() = default;
        ~column_view() = default;
        column_view(column_view const&) = default;
        column_view(column_view&&) = default;
        column_view& operator=(column_view const&) = default;
        column_view& operator=(column_view&&) = default;

        column_view(data_type type,
                    size_type size,
                    void const* data,
                    bitmask_type const* null_mask = nullptr,
                    size_type null_count = unknown_null_count,
                    size_type offset = 0,
                    std::vector<column_view> const& children = {});

        [[nodiscard]] column_view child(size_type child_index) const noexcept { return children_[child_index]; }

        [[nodiscard]] size_type num_children() const noexcept { return children_.size(); }
        auto child_begin() const noexcept { return children_.cbegin(); }
        auto child_end() const noexcept { return children_.cend(); }

        template<typename T, std::enable_if_t<is_numeric<T>() or is_chrono<T>()>>
        column_view(core::span<T const> data)
            : column_view(dataframe::data_type(type_to_id<T>()), data.size(), data.data(), nullptr, 0, 0, {}) {
            assertion_exception_msg(data.size() < static_cast<std::size_t>(std::numeric_limits<size_type>::max()),
                                    "Data exceeds the maximum size of a dataframe view.");
        }

        template<typename T, std::enable_if_t<is_numeric<T>() or is_chrono<T>()>>
        [[nodiscard]] operator core::span<T const>() const {
            assertion_exception_msg(type() == data_type{type_to_id<T>()},
                                    "Device span type must match dataframe view type.");
            assertion_exception_msg(!nullable(), "A nullable dataframe view cannot be converted to a device span.");
            return core::span<T const>(data<T>(), size());
        }

    private:
        friend column_view bit_cast(column_view const& input, data_type type);

        std::vector<column_view> children_{};
    };

    class mutable_column_view : public detail::column_view_base {
    public:
        mutable_column_view() = default;
        ~mutable_column_view() = default;
        mutable_column_view(mutable_column_view const&) = default;
        mutable_column_view(mutable_column_view&&) = default;
        mutable_column_view& operator=(mutable_column_view const&) = default;
        mutable_column_view& operator=(mutable_column_view&&) = default;

        mutable_column_view(data_type type,
                            size_type size,
                            void* data,
                            bitmask_type* null_mask = nullptr,
                            size_type null_count = unknown_null_count,
                            size_type offset = 0,
                            std::vector<mutable_column_view> const& children = {});

        template<typename T = void,
                 typename = std::enable_if_t<std::is_same_v<T, void> or is_rep_layout_compatible<T>()>>
        T* head() const noexcept {
            return const_cast<T*>(detail::column_view_base::head<T>());
        }

        template<typename T, typename = std::enable_if_t<is_rep_layout_compatible<T>()>>
        T* data() const noexcept {
            return const_cast<T*>(detail::column_view_base::data<T>());
        }

        template<typename T, typename = std::enable_if_t<is_rep_layout_compatible<T>()>>
        T* begin() const noexcept {
            return const_cast<T*>(detail::column_view_base::begin<T>());
        }

        template<typename T, typename = std::enable_if_t<is_rep_layout_compatible<T>()>>
        T* end() const noexcept {
            return const_cast<T*>(detail::column_view_base::end<T>());
        }

        [[nodiscard]] bitmask_type* null_mask() const noexcept {
            return const_cast<bitmask_type*>(detail::column_view_base::null_mask());
        }

        void set_null_count(size_type new_null_count);
        [[nodiscard]] mutable_column_view child(size_type child_index) const noexcept {
            return mutable_children[child_index];
        }

        [[nodiscard]] size_type num_children() const noexcept { return mutable_children.size(); }
        auto child_begin() const noexcept { return mutable_children.begin(); }
        auto child_end() const noexcept { return mutable_children.end(); }
        operator column_view() const;

    private:
        friend mutable_column_view bit_cast(mutable_column_view const& input, data_type type);
        std::vector<mutable_column_view> mutable_children;
    };

    size_type count_descendants(column_view parent);
    column_view bit_cast(column_view const& input, data_type type);
    mutable_column_view bit_cast(mutable_column_view const& input, data_type type);

    namespace detail {
        std::size_t shallow_hash(column_view const& input);
        bool is_shallow_equivalent(column_view const& lhs, column_view const& rhs);
    } // namespace detail
} // namespace components::dataframe::column
