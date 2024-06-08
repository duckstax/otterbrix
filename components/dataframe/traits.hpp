#pragma once

#include <string_view>
#include <type_traits>

#include "core/date/timestamps.hpp"
#include <core/date/durations.hpp>

#include "dataframe/dictionary/dictionary.hpp"

#include "core/numbers/fixed_point.hpp"
#include "forward.hpp"
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
        struct is_relationally_comparable_impl : std::false_type {};

        template<typename L, typename R>
        struct is_relationally_comparable_impl<L, R, std::void_t<less_comparable<L, R>, greater_comparable<L, R>>>
            : std::true_type {};

        template<typename L, typename R, typename = void>
        struct is_equality_comparable_impl : std::false_type {};

        template<typename L, typename R>
        struct is_equality_comparable_impl<L, R, std::void_t<equality_comparable<L, R>>> : std::true_type {};

        // has common type
        template<typename AlwaysVoid, typename... Ts>
        struct has_common_type_impl : std::false_type {};

        template<typename... Ts>
        struct has_common_type_impl<std::void_t<std::common_type_t<Ts...>>, Ts...> : std::true_type {};
    } // namespace detail

    template<typename... Ts>
    using has_common_type = typename detail::has_common_type_impl<void, Ts...>::type;

    template<typename... Ts>
    constexpr inline bool has_common_type_v = detail::has_common_type_impl<void, Ts...>::value;

    template<typename T>
    using is_timestamp_t = std::disjunction<std::is_same<core::date::timestamp_day, T>,
                                            std::is_same<core::date::timestamp_s, T>,
                                            std::is_same<core::date::timestamp_ms, T>,
                                            std::is_same<core::date::timestamp_us, T>,
                                            std::is_same<core::date::timestamp_ns, T>>;

    template<typename T>
    using is_duration_t = std::disjunction<std::is_same<core::date::duration_day, T>,
                                           std::is_same<core::date::duration_s, T>,
                                           std::is_same<core::date::duration_ms, T>,
                                           std::is_same<core::date::duration_us, T>,
                                           std::is_same<core::date::duration_ns, T>>;

    template<typename L, typename R>
    constexpr inline bool is_relationally_comparable() {
        return detail::is_relationally_comparable_impl<L, R>::value;
    }

    bool is_relationally_comparable(data_type type);

    template<typename L, typename R>
    constexpr inline bool is_equality_comparable() {
        return detail::is_equality_comparable_impl<L, R>::value;
    }

    bool is_equality_comparable(data_type type);

    template<typename T>
    constexpr inline bool is_numeric() {
        return std::is_arithmetic<T>();
    }

    bool is_numeric(data_type type);

    template<typename T>
    constexpr inline bool is_index_type() {
        return std::is_integral_v<T> and not std::is_same_v<T, bool>;
    }

    bool is_index_type(data_type type);

    template<typename T>
    constexpr inline bool is_unsigned() {
        return std::is_unsigned_v<T>;
    }

    bool is_unsigned(data_type type);

    template<typename Iterator>
    constexpr inline bool is_signed_iterator() {
        return std::is_signed_v<typename std::iterator_traits<Iterator>::value_type>;
    }

    template<typename T>
    constexpr inline bool is_integral() {
        return std::is_integral_v<T>;
    }

    bool is_integral(data_type type);

    template<typename T>
    constexpr inline bool is_floating_point() {
        return std::is_floating_point_v<T>;
    }

    bool is_floating_point(data_type type);

    template<typename T>
    constexpr inline bool is_byte() {
        return std::is_same_v<std::remove_cv_t<T>, std::byte>;
    }

    template<typename T>
    constexpr inline bool is_boolean() {
        return std::is_same_v<T, bool>;
    }

    bool is_boolean(data_type type);

    template<typename T>
    constexpr inline bool is_timestamp() {
        return is_timestamp_t<T>::value;
    }

    bool is_timestamp(data_type type);

    template<typename T>
    constexpr inline bool is_fixed_point() {
        return std::is_same_v<core::numbers::decimal32, T> || std::is_same_v<core::numbers::decimal64, T> ||
               std::is_same_v<core::numbers::decimal128, T>;
    }

    bool is_fixed_point(data_type type);

    template<typename T>
    constexpr inline bool is_duration() {
        return is_duration_t<T>::value;
    }

    bool is_duration(data_type type);

    template<typename T>
    constexpr inline bool is_chrono() {
        return is_duration<T>() || is_timestamp<T>();
    }

    bool is_chrono(data_type type);

    template<typename T>
    constexpr bool is_rep_layout_compatible() {
        return is_numeric<T>() || is_chrono<T>() || is_boolean<T>() || is_byte<T>();
    }

    template<typename T>
    constexpr inline bool is_dictionary() {
        return std::is_same_v<dictionary::dictionary32, T>;
    }

    bool is_dictionary(data_type type);

    template<typename T>
    constexpr inline bool is_fixed_width() {
        return is_numeric<T>() || is_chrono<T>() || is_fixed_point<T>();
    }

    bool is_fixed_width(data_type type);

    template<typename T>
    constexpr inline bool is_compound() {
        return std::is_same_v<T, std::string_view> or std::is_same_v<T, dictionary::dictionary32> or
               std::is_same_v<T, lists::list_view> or std::is_same_v<T, structs::struct_view>;
    }

    bool is_compound(data_type type);

    template<typename T>
    constexpr inline bool is_nested() {
        return std::is_same_v<T, lists::list_view> || std::is_same_v<T, structs::struct_view>;
    }

    bool is_nested(data_type type);

    bool is_bit_castable(data_type from, data_type to);

    template<typename From, typename To>
    struct is_convertible : std::is_convertible<From, To> {};

    template<typename Duration1, typename Duration2>
    struct is_convertible<core::date::detail::timestamp<Duration1>, core::date::detail::timestamp<Duration2>>
        : std::is_convertible<typename core::date::detail::time_point<Duration1>::duration,
                              typename core::date::detail::time_point<Duration2>::duration> {};

} // namespace components::dataframe
