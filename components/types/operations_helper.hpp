#pragma once

#include "types.hpp"
#include <boost/math/special_functions/factorials.hpp>

namespace components::types {

    template<typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
    static inline bool is_equals(T x, T y) {
        return std::fabs(x - y) < std::numeric_limits<T>::epsilon();
    }

    // This could be useful in other places, but for now it is here
    // Default only accepts int as amount
    constexpr int128_t operator<<(int128_t lhs, int128_t amount) { return lhs << static_cast<int>(amount); }
    constexpr int128_t operator>>(int128_t lhs, int128_t amount) { return lhs >> static_cast<int>(amount); }

    // there is no std::shift_left operator
    template<typename T = void>
    struct shift_left;
    template<typename T = void>
    struct shift_right;
    template<typename T = void>
    struct pow;
    template<typename T = void>
    struct sqrt;
    template<typename T = void>
    struct cbrt;
    template<typename T = void>
    struct fact;
    template<typename T = void>
    struct abs;

    template<>
    struct shift_left<void> {
        template<typename T, typename U>
        constexpr auto operator()(T&& t, U&& u) const {
            return std::forward<T>(t) << std::forward<U>(u);
        }
    };

    template<>
    struct shift_right<void> {
        template<typename T, typename U>
        constexpr auto operator()(T&& t, U&& u) const {
            return std::forward<T>(t) >> std::forward<U>(u);
        }
    };

    template<>
    struct pow<void> {
        template<typename T, typename U>
        constexpr auto operator()(T&& t, U&& u) const {
            if constexpr (std::is_same<T, int128_t>::value) {
                return t ^ u;
            } else {
                return std::pow(std::forward<T>(t), std::forward<U>(u));
            }
        }
    };

    template<>
    struct sqrt<void> {
        template<typename T>
        constexpr auto operator()(T&& x) const {
            return std::sqrt(std::forward<T>(x));
        }
    };

    template<>
    struct cbrt<void> {
        template<typename T>
        constexpr auto operator()(T&& x) const {
            return std::cbrt(std::forward<T>(x));
        }
    };

    template<>
    struct fact<void> {
        template<typename T>
        constexpr auto operator()(T&& x) const {
            return boost::math::factorial<double>(std::forward<T>(x));
        }
    };

    template<>
    struct abs<void> {
        template<typename T>
        constexpr auto operator()(T&& x) const {
            if constexpr (std::is_same<T, int128_t>::value) {
                return x < 0 ? -x : x;
            } else {
                return std::abs<T>(std::forward<T>(x));
            }
        }
    };

} // namespace components::types