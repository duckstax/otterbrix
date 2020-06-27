#pragma once

#include <cstdlib>
#include <cassert>
#include <algorithm>

#include <boost/utility/string_view.hpp>

// mutable_buffer, const_buffer and buffer are based on
// the Networking TS specification, draft:
// http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2018/n4771.pdf
namespace components {
#define IS_TRIVIALLY_COPYABLE(T) std::is_trivially_copyable<T>::value

    class const_buffer;

    class mutable_buffer;

    namespace detail {
        template<class T>
        struct is_buffer {
            static constexpr bool value = std::is_same<T, const_buffer>::value || std::is_same<T, mutable_buffer>::value;
        };

        template<class T>
        struct is_pod_like {
            // NOTE: The networking draft N4771 section 16.11 requires
            // T in the buffer functions below to be
            // trivially copyable OR standard layout.
            // Here we decide to be conservative and require both.
            static constexpr bool value = IS_TRIVIALLY_COPYABLE(T) && std::is_standard_layout<T>::value;
        };

        template<class C>
        constexpr auto seq_size(const C &c) noexcept -> decltype(c.size()) {
            return c.size();
        }

        template<class T, size_t N>
        constexpr size_t seq_size(const T (&/*array*/)[N]) noexcept {
            return N;
        }

        template<class Seq>
        auto buffer_contiguous_sequence(Seq &&seq) noexcept
        -> decltype(buffer(std::addressof(*std::begin(seq)), size_t{})) {
            using T = typename std::remove_cv<
                    typename std::remove_reference<decltype(*std::begin(seq))>::type>::type;
            static_assert(detail::is_pod_like<T>::value, "T must be POD");

            const auto size = seq_size(seq);
            return buffer(size != 0u ? std::addressof(*std::begin(seq)) : nullptr,
                          size * sizeof(T));
        }

        template<class Seq>
        auto buffer_contiguous_sequence(Seq &&seq, size_t n_bytes) noexcept
        -> decltype(buffer_contiguous_sequence(seq)) {
            using T = typename std::remove_cv<
                    typename std::remove_reference<decltype(*std::begin(seq))>::type>::type;
            static_assert(detail::is_pod_like<T>::value, "T must be POD");

            const auto size = seq_size(seq);
            return buffer(size != 0u ? std::addressof(*std::begin(seq)) : nullptr,
                          (std::min)(size * sizeof(T), n_bytes));
        }

    } // namespace detail
}

