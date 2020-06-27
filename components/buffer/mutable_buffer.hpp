#pragma once

#include <cstdlib>
#include <cassert>
#include <algorithm>

#include <boost/utility/string_view.hpp>

#include "type_traits.hpp"

namespace components {
    class mutable_buffer final {
    public:
        constexpr mutable_buffer() noexcept
                : _data(nullptr), _size(0) {}

        constexpr mutable_buffer(void *p, size_t n) noexcept
                : _data(p), _size(n) {
            assert(p != nullptr || n == 0);
        }

        constexpr void *data() const noexcept { return _data; }

        constexpr size_t size() const noexcept { return _size; }

        mutable_buffer &operator+=(size_t n) noexcept {
            // (std::min) is a workaround for when a min macro is defined
            const auto shift = (std::min)(n, _size);
            _data = static_cast<char *>(_data) + shift;
            _size -= shift;
            return *this;
        }

    private:
        void *_data;
        size_t _size;
    };

    inline mutable_buffer operator+(const mutable_buffer &mb, size_t n) noexcept {
        return mutable_buffer(static_cast<char *>(mb.data()) + (std::min)(n, mb.size()),
                              mb.size() - (std::min)(n, mb.size()));
    }

    inline mutable_buffer operator+(size_t n, const mutable_buffer &mb) noexcept {
        return mb + n;
    }

    constexpr mutable_buffer buffer(void *p, size_t n) noexcept {
        return mutable_buffer(p, n);
    }

    constexpr mutable_buffer buffer(const mutable_buffer &mb) noexcept {
        return mb;
    }

    inline mutable_buffer buffer(const mutable_buffer &mb, size_t n) noexcept {
        return mutable_buffer(mb.data(), (std::min)(mb.size(), n));
    }

// C array
    template<class T, size_t N>
    mutable_buffer buffer(T (&data)[N]) noexcept {
        return detail::buffer_contiguous_sequence(data);
    }

    template<class T, size_t N>
    mutable_buffer buffer(T (&data)[N], size_t n_bytes) noexcept {
        return detail::buffer_contiguous_sequence(data, n_bytes);
    }

// std::array
    template<class T, size_t N>
    mutable_buffer buffer(std::array<T, N> &data) noexcept {
        return detail::buffer_contiguous_sequence(data);
    }

    template<class T, size_t N>
    mutable_buffer buffer(std::array<T, N> &data, size_t n_bytes) noexcept {
        return detail::buffer_contiguous_sequence(data, n_bytes);
    }

// std::vector
    template<class T, class Allocator>
    mutable_buffer buffer(std::vector<T, Allocator> &data) noexcept {
        return detail::buffer_contiguous_sequence(data);
    }

    template<class T, class Allocator>
    mutable_buffer buffer(std::vector<T, Allocator> &data, size_t n_bytes) noexcept {
        return detail::buffer_contiguous_sequence(data, n_bytes);
    }

// std::basic_string
    template<class T, class Traits, class Allocator>
    mutable_buffer buffer(std::basic_string<T, Traits, Allocator> &data) noexcept {
        return detail::buffer_contiguous_sequence(data);
    }

    template<class T, class Traits, class Allocator>
    mutable_buffer buffer(std::basic_string<T, Traits, Allocator> &data,
                          size_t n_bytes) noexcept {
        return detail::buffer_contiguous_sequence(data, n_bytes);
    }
}
