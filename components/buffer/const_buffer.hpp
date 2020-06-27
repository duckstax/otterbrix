#pragma once

#include <cstdlib>
#include <cassert>
#include <algorithm>

#include <boost/utility/string_view.hpp>
#include "mutable_buffer.hpp"

namespace components {
    class const_buffer {
    public:
        constexpr const_buffer() noexcept
                : _data(nullptr), _size(0) {}

        constexpr const_buffer(const void *p, size_t n) noexcept
                : _data(p), _size(n) {
            assert(p != nullptr || n == 0);
        }

        constexpr const_buffer(const mutable_buffer &mb) noexcept
                : _data(mb.data()), _size(mb.size()) {
        }

        constexpr const void *data() const noexcept { return _data; }

        constexpr size_t size() const noexcept { return _size; }

        const_buffer &operator+=(size_t n) noexcept {
            const auto shift = (std::min)(n, _size);
            _data = static_cast<const char *>(_data) + shift;
            _size -= shift;
            return *this;
        }

    private:
        const void *_data;
        size_t _size;
    };

    inline const_buffer operator+(const const_buffer &cb, size_t n) noexcept {
        return const_buffer(static_cast<const char *>(cb.data()) + (std::min)(n, cb.size()),
                            cb.size() - (std::min)(n, cb.size()));
    }

    inline const_buffer operator+(size_t n, const const_buffer &cb) noexcept {
        return cb + n;
    }

    constexpr const_buffer buffer(const void *p, size_t n) noexcept {
        return const_buffer(p, n);
    }

    constexpr const_buffer buffer(const const_buffer &cb) noexcept {
        return cb;
    }

    inline const_buffer buffer(const const_buffer &cb, size_t n) noexcept {
        return const_buffer(cb.data(), (std::min)(cb.size(), n));
    }

// Buffer for a string literal (null terminated)
// where the buffer size excludes the terminating character.
// Equivalent to buffer(std::string_view("...")).
    template<class Char, size_t N>
    constexpr const_buffer str_buffer(const Char (&data)[N]) noexcept {
        static_assert(detail::is_pod_like<Char>::value, "Char must be POD");
        assert(data[N - 1] == Char{0});
        return const_buffer(static_cast<const Char *>(data), (N - 1) * sizeof(Char));
    }

// C array

    template<class T, size_t N>
    const_buffer buffer(const T (&data)[N]) noexcept {
        return detail::buffer_contiguous_sequence(data);
    }

    template<class T, size_t N>
    const_buffer buffer(const T (&data)[N], size_t n_bytes) noexcept {
        return detail::buffer_contiguous_sequence(data, n_bytes);
    }

    template<class T, size_t N>
    const_buffer buffer(std::array<const T, N> &data) noexcept {
        return detail::buffer_contiguous_sequence(data);
    }

    template<class T, size_t N>
    const_buffer buffer(std::array<const T, N> &data, size_t n_bytes) noexcept {
        return detail::buffer_contiguous_sequence(data, n_bytes);
    }

    template<class T, size_t N>
    const_buffer buffer(const std::array<T, N> &data) noexcept {
        return detail::buffer_contiguous_sequence(data);
    }

    template<class T, size_t N>
    const_buffer buffer(const std::array<T, N> &data, size_t n_bytes) noexcept {
        return detail::buffer_contiguous_sequence(data, n_bytes);
    }

    template<class T, class Allocator>
    const_buffer buffer(const std::vector<T, Allocator> &data) noexcept {
        return detail::buffer_contiguous_sequence(data);
    }

    template<class T, class Allocator>
    const_buffer buffer(const std::vector<T, Allocator> &data, size_t n_bytes) noexcept {
        return detail::buffer_contiguous_sequence(data, n_bytes);
    }

    template<class T, class Traits, class Allocator>
    const_buffer buffer(const std::basic_string<T, Traits, Allocator> &data) noexcept {
        return detail::buffer_contiguous_sequence(data);
    }

    template<class T, class Traits, class Allocator>
    const_buffer buffer(const std::basic_string<T, Traits, Allocator> &data,
                        size_t n_bytes) noexcept {
        return detail::buffer_contiguous_sequence(data, n_bytes);
    }

    template<class T, class Traits>
    const_buffer buffer(boost::basic_string_view<T, Traits> data) noexcept {
        return detail::buffer_contiguous_sequence(data);
    }

    template<class T, class Traits>
    const_buffer buffer(boost::basic_string_view<T, Traits> data, size_t n_bytes) noexcept {
        return detail::buffer_contiguous_sequence(data, n_bytes);
    }

    namespace literals {
        constexpr const_buffer operator "" _buf(const char *str, size_t len) noexcept {
            return const_buffer(str, len * sizeof(char));
        }

        constexpr const_buffer operator "" _buf(const wchar_t *str, size_t len) noexcept {
            return const_buffer(str, len * sizeof(wchar_t));
        }

        constexpr const_buffer operator "" _buf(const char16_t *str, size_t len) noexcept {
            return const_buffer(str, len * sizeof(char16_t));
        }

        constexpr const_buffer operator "" _buf(const char32_t *str, size_t len) noexcept {
            return const_buffer(str, len * sizeof(char32_t));
        }
    }
}