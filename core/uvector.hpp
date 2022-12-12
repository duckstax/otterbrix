#pragma once

#include "buffer.hpp"

#include <cstddef>
#include <vector>

namespace core {

    template<typename T>
    class uvector {
        static_assert(std::is_trivially_copyable<T>::value, "uvector only supports types that are trivially copyable.");

    public:
        using value_type = T;
        using size_type = std::size_t;
        using reference = value_type&;
        using const_reference = value_type const&;
        using pointer = value_type*;
        using const_pointer = value_type const*;
        using iterator = pointer;
        using const_iterator = const_pointer;

        ~uvector() = default;
        uvector(uvector&&) noexcept = default;
        uvector& operator=(uvector&&) noexcept = default;
        uvector(uvector const&) = delete;
        uvector& operator=(uvector const&) = delete;

        uvector() = delete;

        uvector(std::pmr::memory_resource* mr, std::size_t size)
            : _storage{mr, elements_to_bytes(size)} {
        }

        uvector(std::pmr::memory_resource* mr, uvector const& other)
            : _storage{mr, other._storage} {
        }

        [[nodiscard]] pointer element_ptr(std::size_t element_index) noexcept {
            assert(element_index < size());
            return data() + element_index;
        }

        [[nodiscard]] const_pointer element_ptr(std::size_t element_index) const noexcept {
            assert(element_index < size());
            return data() + element_index;
        }

        void set_element_async(std::size_t element_index, value_type const& value) {
            assert(element_index < size());

            if constexpr (std::is_same<value_type, bool>::value) {
                memset(element_ptr(element_index), value, sizeof(value));
                return;
            }

            if constexpr (std::is_fundamental<value_type>::value) {
                if (value == value_type{0}) {
                    set_element_to_zero_async(element_index);
                    return;
                }
            }

            std::memcpy(element_ptr(element_index), &value, sizeof(value));
        }

        void set_element_async(std::size_t, value_type const&&) = delete;
        void set_element_to_zero_async(std::size_t element_index) {
            assert(element_index < size());
            assert(memset(element_ptr(element_index), 0, sizeof(value_type)));
        }

        void set_element(std::size_t element_index, T const& value) {
            set_element_async(element_index, value);
        }

        [[nodiscard]] value_type element(std::size_t element_index) const {
            assert(element_index < size());
            value_type value;
            std::memcpy(&value, element_ptr(element_index), sizeof(value));
            return value;
        }
        [[nodiscard]] value_type front_element() const {
            return element(0);
        }
        [[nodiscard]] value_type back_element() const {
            return element(size() - 1);
        }
        void reserve(std::size_t new_capacity) {
            _storage.reserve(elements_to_bytes(new_capacity));
        }
        void resize(std::size_t new_size) {
            _storage.resize(elements_to_bytes(new_size));
        }
        void shrink_to_fit() { _storage.shrink_to_fit(); }
        buffer release() noexcept { return std::move(_storage); }
        [[nodiscard]] std::size_t capacity() const noexcept {
            return bytes_to_elements(_storage.capacity());
        }
        [[nodiscard]] pointer data() noexcept { return static_cast<pointer>(_storage.data()); }
        [[nodiscard]] const_pointer data() const noexcept {
            return static_cast<const_pointer>(_storage.data());
        }
        [[nodiscard]] iterator begin() noexcept { return data(); }
        [[nodiscard]] const_iterator cbegin() const noexcept { return data(); }
        [[nodiscard]] const_iterator begin() const noexcept { return cbegin(); }
        [[nodiscard]] iterator end() noexcept { return data() + size(); }
        [[nodiscard]] const_iterator cend() const noexcept { return data() + size(); }
        [[nodiscard]] const_iterator end() const noexcept { return cend(); }
        [[nodiscard]] std::size_t size() const noexcept { return bytes_to_elements(_storage.size()); }

        [[nodiscard]] std::int64_t ssize() const noexcept {
            assert(size() < static_cast<std::size_t>(std::numeric_limits<int64_t>::max()));
            return static_cast<int64_t>(size());
        }

        [[nodiscard]] bool is_empty() const noexcept { return size() == 0; }

        [[nodiscard]] std::pmr::memory_resource* memory_resource() const noexcept {
            return _storage.memory_resource();
        }

    private:
        buffer _storage{};

        [[nodiscard]] std::size_t constexpr elements_to_bytes(std::size_t num_elements) const noexcept {
            return num_elements * sizeof(value_type);
        }

        [[nodiscard]] std::size_t constexpr bytes_to_elements(std::size_t num_bytes) const noexcept {
            return num_bytes / sizeof(value_type);
        }
    };
} // namespace core
