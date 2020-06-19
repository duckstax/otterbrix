#pragma once

// mutable_buffer, const_buffer and buffer are based on
// the Networking TS specification, draft:
// http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2018/n4771.pdf

class mutable_buffer final {
public:
    constexpr mutable_buffer() noexcept
        : _data(nullptr)
        , _size(0) {}
    constexpr mutable_buffer(void* p, size_t n) noexcept
        : _data(p)
        , _size(n) {
        assert(p != nullptr || n == 0);
    }

    constexpr void* data() const noexcept { return _data; }
    constexpr size_t size() const noexcept { return _size; }
    mutable_buffer& operator+=(size_t n) noexcept {
        // (std::min) is a workaround for when a min macro is defined
        const auto shift = (std::min)(n, _size);
        _data = static_cast<char*>(_data) + shift;
        _size -= shift;
        return *this;
    }

private:
    void* _data;
    size_t _size;
};

inline mutable_buffer operator+(const mutable_buffer& mb, size_t n) noexcept {
    return mutable_buffer(static_cast<char*>(mb.data()) + (std::min)(n, mb.size()),
                          mb.size() - (std::min)(n, mb.size()));
}
inline mutable_buffer operator+(size_t n, const mutable_buffer& mb) noexcept {
    return mb + n;
}

class const_buffer {
public:
    constexpr const_buffer() noexcept
        : _data(nullptr)
        , _size(0) {}
    constexpr const_buffer(const void* p, size_t n) noexcept
        : _data(p)
        , _size(n) {
        assert(p != nullptr || n == 0);
    }
    constexpr const_buffer(const mutable_buffer& mb) noexcept
        : _data(mb.data())
        , _size(mb.size()) {
    }

    constexpr const void* data() const noexcept { return _data; }
    constexpr size_t size() const noexcept { return _size; }
    const_buffer& operator+=(size_t n) noexcept {
        const auto shift = (std::min)(n, _size);
        _data = static_cast<const char*>(_data) + shift;
        _size -= shift;
        return *this;
    }

private:
    const void* _data;
    size_t _size;
};

inline const_buffer operator+(const const_buffer& cb, size_t n) noexcept {
    return const_buffer(static_cast<const char*>(cb.data()) + (std::min)(n, cb.size()),
                        cb.size() - (std::min)(n, cb.size()));
}
inline const_buffer operator+(size_t n, const const_buffer& cb) noexcept {
    return cb + n;
}

// buffer creation

constexpr mutable_buffer buffer(void* p, size_t n) noexcept {
    return mutable_buffer(p, n);
}
constexpr const_buffer buffer(const void* p, size_t n) noexcept {
    return const_buffer(p, n);
}
constexpr mutable_buffer buffer(const mutable_buffer& mb) noexcept {
    return mb;
}
inline mutable_buffer buffer(const mutable_buffer& mb, size_t n) noexcept {
    return mutable_buffer(mb.data(), (std::min)(mb.size(), n));
}
constexpr const_buffer buffer(const const_buffer& cb) noexcept {
    return cb;
}
inline const_buffer buffer(const const_buffer& cb, size_t n) noexcept {
    return const_buffer(cb.data(), (std::min)(cb.size(), n));
}

namespace detail {
    template<class T>
    struct is_buffer {
        static constexpr bool value =
            std::is_same<T, const_buffer>::value || std::is_same<T, mutable_buffer>::value;
    };

    template<class T>
    struct is_pod_like {
        // NOTE: The networking draft N4771 section 16.11 requires
        // T in the buffer functions below to be
        // trivially copyable OR standard layout.
        // Here we decide to be conservative and require both.
        static constexpr bool value =
            ZMQ_IS_TRIVIALLY_COPYABLE(T) && std::is_standard_layout<T>::value;
    };

    template<class C>
    constexpr auto seq_size(const C& c) noexcept -> decltype(c.size()) {
        return c.size();
    }
    template<class T, size_t N>
    constexpr size_t seq_size(const T (&/*array*/)[N]) noexcept {
        return N;
    }

    template<class Seq>
    auto buffer_contiguous_sequence(Seq&& seq) noexcept
        -> decltype(buffer(std::addressof(*std::begin(seq)), size_t{})) {
        using T = typename std::remove_cv<
            typename std::remove_reference<decltype(*std::begin(seq))>::type>::type;
        static_assert(detail::is_pod_like<T>::value, "T must be POD");

        const auto size = seq_size(seq);
        return buffer(size != 0u ? std::addressof(*std::begin(seq)) : nullptr,
                      size * sizeof(T));
    }
    template<class Seq>
    auto buffer_contiguous_sequence(Seq&& seq, size_t n_bytes) noexcept
        -> decltype(buffer_contiguous_sequence(seq)) {
        using T = typename std::remove_cv<
            typename std::remove_reference<decltype(*std::begin(seq))>::type>::type;
        static_assert(detail::is_pod_like<T>::value, "T must be POD");

        const auto size = seq_size(seq);
        return buffer(size != 0u ? std::addressof(*std::begin(seq)) : nullptr,
                      (std::min)(size * sizeof(T), n_bytes));
    }

} // namespace detail

// C array
template<class T, size_t N>
mutable_buffer buffer(T (&data)[N]) noexcept {
    return detail::buffer_contiguous_sequence(data);
}
template<class T, size_t N>
mutable_buffer buffer(T (&data)[N], size_t n_bytes) noexcept {
    return detail::buffer_contiguous_sequence(data, n_bytes);
}
template<class T, size_t N>
const_buffer buffer(const T (&data)[N]) noexcept {
    return detail::buffer_contiguous_sequence(data);
}
template<class T, size_t N>
const_buffer buffer(const T (&data)[N], size_t n_bytes) noexcept {
    return detail::buffer_contiguous_sequence(data, n_bytes);
}
// std::array
template<class T, size_t N>
mutable_buffer buffer(std::array<T, N>& data) noexcept {
    return detail::buffer_contiguous_sequence(data);
}
template<class T, size_t N>
mutable_buffer buffer(std::array<T, N>& data, size_t n_bytes) noexcept {
    return detail::buffer_contiguous_sequence(data, n_bytes);
}
template<class T, size_t N>
const_buffer buffer(std::array<const T, N>& data) noexcept {
    return detail::buffer_contiguous_sequence(data);
}
template<class T, size_t N>
const_buffer buffer(std::array<const T, N>& data, size_t n_bytes) noexcept {
    return detail::buffer_contiguous_sequence(data, n_bytes);
}
template<class T, size_t N>
const_buffer buffer(const std::array<T, N>& data) noexcept {
    return detail::buffer_contiguous_sequence(data);
}
template<class T, size_t N>
const_buffer buffer(const std::array<T, N>& data, size_t n_bytes) noexcept {
    return detail::buffer_contiguous_sequence(data, n_bytes);
}
// std::vector
template<class T, class Allocator>
mutable_buffer buffer(std::vector<T, Allocator>& data) noexcept {
    return detail::buffer_contiguous_sequence(data);
}
template<class T, class Allocator>
mutable_buffer buffer(std::vector<T, Allocator>& data, size_t n_bytes) noexcept {
    return detail::buffer_contiguous_sequence(data, n_bytes);
}
template<class T, class Allocator>
const_buffer buffer(const std::vector<T, Allocator>& data) noexcept {
    return detail::buffer_contiguous_sequence(data);
}
template<class T, class Allocator>
const_buffer buffer(const std::vector<T, Allocator>& data, size_t n_bytes) noexcept {
    return detail::buffer_contiguous_sequence(data, n_bytes);
}
// std::basic_string
template<class T, class Traits, class Allocator>
mutable_buffer buffer(std::basic_string<T, Traits, Allocator>& data) noexcept {
    return detail::buffer_contiguous_sequence(data);
}
template<class T, class Traits, class Allocator>
mutable_buffer buffer(std::basic_string<T, Traits, Allocator>& data,
                      size_t n_bytes) noexcept {
    return detail::buffer_contiguous_sequence(data, n_bytes);
}
template<class T, class Traits, class Allocator>
const_buffer buffer(const std::basic_string<T, Traits, Allocator>& data) noexcept {
    return detail::buffer_contiguous_sequence(data);
}
template<class T, class Traits, class Allocator>
const_buffer buffer(const std::basic_string<T, Traits, Allocator>& data,
                    size_t n_bytes) noexcept {
    return detail::buffer_contiguous_sequence(data, n_bytes);
}

template<class T, class Traits>
const_buffer buffer(std::basic_string_view<T, Traits> data) noexcept {
    return detail::buffer_contiguous_sequence(data);
}
template<class T, class Traits>
const_buffer buffer(std::basic_string_view<T, Traits> data, size_t n_bytes) noexcept {
    return detail::buffer_contiguous_sequence(data, n_bytes);
}

// Buffer for a string literal (null terminated)
// where the buffer size excludes the terminating character.
// Equivalent to zmq::buffer(std::string_view("...")).
template<class Char, size_t N>
constexpr const_buffer str_buffer(const Char (&data)[N]) noexcept {
    static_assert(detail::is_pod_like<Char>::value, "Char must be POD");
    assert(data[N - 1] == Char{0});
    return const_buffer(static_cast<const Char*>(data), (N - 1) * sizeof(Char));
}

namespace literals {
    constexpr const_buffer operator"" _buf(const char* str, size_t len) noexcept {
        return const_buffer(str, len * sizeof(char));
    }
    constexpr const_buffer operator"" _buf(const wchar_t* str, size_t len) noexcept {
        return const_buffer(str, len * sizeof(wchar_t));
    }
    constexpr const_buffer operator"" _zbuf(const char16_t* str, size_t len) noexcept {
        return const_buffer(str, len * sizeof(char16_t));
    }
    constexpr const_buffer operator"" _buf(const char32_t* str, size_t len) noexcept {
        return const_buffer(str, len * sizeof(char32_t));
    }
} // namespace literals