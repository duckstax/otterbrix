#pragma once

#include <type_traits>
#include <string_view>

#include <core/date/durations.hpp>
#include <core/date/timestamps.hpp>
#include <core/numbers/fixed_point.hpp>
#include <core/dictionary.hpp>

#include "types.hpp"

namespace components::dataframe {

    template<typename L, typename R>
    using less_comparable = decltype(std::declval<L>() < std::declval<R>());

    template<typename L, typename R>
    using greater_comparable = decltype(std::declval<L>() > std::declval<R>());

    template<typename L, typename R>
    using equality_comparable = decltype(std::declval<L>() == std::declval<R>());

    namespace detail {
        template<typename L, typename R, typename = void>
        struct is_relationally_comparable_impl : std::false_type {
        };

        template<typename L, typename R>
        struct is_relationally_comparable_impl<L, R, std::void_t<less_comparable<L, R>, greater_comparable<L, R>>>
            : std::true_type {
        };

        template<typename L, typename R, typename = void>
        struct is_equality_comparable_impl : std::false_type {
        };

        template<typename L, typename R>
        struct is_equality_comparable_impl<L, R, std::void_t<equality_comparable<L, R>>> : std::true_type {
        };

        // has common type
        template<typename AlwaysVoid, typename... Ts>
        struct has_common_type_impl : std::false_type {
        };

        template<typename... Ts>
        struct has_common_type_impl<std::void_t<std::common_type_t<Ts...>>, Ts...> : std::true_type {
        };
    } // namespace detail

    template<typename... Ts>
    using has_common_type = typename detail::has_common_type_impl<void, Ts...>::type;

    template<typename... Ts>
    constexpr inline bool has_common_type_v = detail::has_common_type_impl<void, Ts...>::value;


    template<typename T>
    using is_timestamp_t = std::disjunction<std::is_same<::core::date::timestamp_day, T>,
                                            std::is_same<::core::date::timestamp_s, T>,
                                            std::is_same<::core::date::timestamp_ms, T>,
                                            std::is_same<::core::date::timestamp_us, T>,
                                            std::is_same<::core::date::timestamp_ns, T>>;

    template<typename T>
    using is_duration_t = std::disjunction<std::is_same<::core::date::duration_day, T>,
                                           std::is_same<::core::date::duration_s, T>,
                                           std::is_same<::core::date::duration_ms, T>,
                                           std::is_same<::core::date::duration_us, T>,
                                           std::is_same<::core::date::duration_ns, T>>;

    template<typename L, typename R>
    constexpr inline bool is_relationally_comparable() {
        return detail::is_relationally_comparable_impl<L, R>::value;
    }

    template<typename L, typename R>
    constexpr inline bool is_equality_comparable() {
        return detail::is_equality_comparable_impl<L, R>::value;
    }

    template<typename T>
    constexpr inline bool is_numeric() {
        return std::is_arithmetic<T>();
    }

    template<typename T>
    constexpr inline bool is_index_type() {
        return std::is_integral_v<T> and not std::is_same_v<T, bool>;
    }

    template<typename T>
    constexpr inline bool is_unsigned() {
        return std::is_unsigned_v<T>;
    }

    template<typename Iterator>
    constexpr inline bool is_signed_iterator() {
        return std::is_signed_v<typename std::iterator_traits<Iterator>::value_type>;
    }

    template<typename T>
    constexpr inline bool is_integral() {
        return std::is_integral_v<T>;
    }

    template<typename T>
    constexpr inline bool is_floating_point() {
        return std::is_floating_point_v<T>;
    }

    template<typename T>
    constexpr inline bool is_byte() {
        return std::is_same_v<std::remove_cv_t<T>, std::byte>;
    }

    template<typename T>
    constexpr inline bool is_boolean() {
        return std::is_same_v<T, bool>;
    }

    template<typename T>
    constexpr inline bool is_timestamp() {
        return is_timestamp_t<T>::value;
    }


    template<typename T>
    constexpr inline bool is_fixed_point() {
        return std::is_same_v<core::numbers::decimal32, T> || std::is_same_v<core::numbers::decimal64, T> ||
               std::is_same_v<core::numbers::decimal128, T>;
    }

    template<typename T>
    constexpr inline bool is_duration() {
        return is_duration_t<T>::value;
    }

    template<typename T>
    constexpr inline bool is_chrono() {
        return is_duration<T>() || is_timestamp<T>();
    }

    template<typename T>
    constexpr bool is_rep_layout_compatible() {
        return is_numeric<T>() || is_chrono<T>() || is_boolean<T>() || is_byte<T>();
    }

    template<typename T>
    constexpr inline bool is_dictionary() {
        return std::is_same_v<core::dictionary32, T>;
    }

    template<typename T>
    constexpr inline bool is_fixed_width() {
        return is_numeric<T>() || is_chrono<T>() || is_fixed_point<T>();
    }

    template<typename From, typename To>
    struct is_convertible : std::is_convertible<From, To> {};

    template<typename Duration1, typename Duration2>
    struct is_convertible<::core::date::detail::timestamp<Duration1>, ::core::date::detail::timestamp<Duration2>>
        : std::is_convertible<typename ::core::date::detail::time_point<Duration1>::duration,
                              typename ::core::date::detail::time_point<Duration2>::duration> {
    };

} // namespace dataframe
