#pragma once

#include <cstdint>

#include <string_view>

#include "core/numbers/fixed_point.hpp"
#include "dictionary/dictionary.hpp"
#include "forward.hpp"
#include "types.hpp"

namespace components::dataframe {

    template<typename T>
    inline constexpr type_id type_to_id() {
        return type_id::empty;
    }

    struct type_to_name {
        template<typename T>
        inline std::string operator()() {
            return "void";
        }
    };

    template<type_id t>
    struct id_to_type_impl {
        using type = void;
    };

    template<type_id Id>
    using id_to_type = typename id_to_type_impl<Id>::type;

    // clang-format off
    template <typename T>
    using device_storage_type_t =
      std::conditional_t<std::is_same_v<core::numbers::decimal32,  T>, int32_t,
      std::conditional_t<std::is_same_v<core::numbers::decimal64,  T>, int64_t,
      std::conditional_t<std::is_same_v<core::numbers::decimal128, T>, absl::int128, T>>>;
    // clang-format on

    template<typename T>
    constexpr bool type_id_matches_device_storage_type(type_id id) {
        return (id == type_id::decimal32 && std::is_same_v<T, int32_t>) ||
               (id == type_id::decimal64 && std::is_same_v<T, int64_t>) ||
               (id == type_id::decimal128 && std::is_same_v<T, absl::int128>) || id == type_to_id<T>();
    }

#define stringify_detail(x) #x
#define stringify(x) stringify_detail(x)

#ifndef type_mapping
#define type_mapping(Type, Id)                                                                                         \
    template<>                                                                                                         \
    constexpr inline type_id type_to_id<Type>() {                                                                      \
        return Id;                                                                                                     \
    }                                                                                                                  \
    template<>                                                                                                         \
    inline std::string type_to_name::operator()<Type>() {                                                              \
        return stringify(Type);                                                                                        \
    }                                                                                                                  \
    template<>                                                                                                         \
    struct id_to_type_impl<Id> {                                                                                       \
        using type = Type;                                                                                             \
    };
#endif

    // clang-format off
    type_mapping(bool, type_id::bool8)
    type_mapping(std::int8_t, type_id::int8)
    type_mapping(std::int16_t, type_id::int16)
    type_mapping(std::int32_t, type_id::int32)
    type_mapping(std::int64_t, type_id::int64)
    type_mapping(std::uint8_t, type_id::uint8)
    type_mapping(std::uint16_t, type_id::uint16)
    type_mapping(std::uint32_t, type_id::uint32)
    type_mapping(std::uint64_t, type_id::uint64)
    type_mapping(float, type_id::float32)
    type_mapping(double, type_id::float64)
    type_mapping(std::string_view, type_id::string)
    type_mapping(core::date::timestamp_day , type_id::timestamp_days)
    type_mapping(core::date::timestamp_s, type_id::timestamp_seconds)
    type_mapping(core::date::timestamp_ms, type_id::timestamp_milliseconds)
    type_mapping(core::date::timestamp_us, type_id::timestamp_microseconds)
    type_mapping(core::date::timestamp_ns, type_id::timestamp_nanoseconds)
    type_mapping(core::date::duration_day, type_id::duration_days)
    type_mapping(core::date::duration_s, type_id::duration_seconds)
    type_mapping(core::date::duration_ms, type_id::duration_milliseconds)
    type_mapping(core::date::duration_us, type_id::duration_microseconds)
    type_mapping(core::date::duration_ns, type_id::duration_nanoseconds)
    type_mapping(dictionary::dictionary32, type_id::dictionary32)
    type_mapping(lists::list_view, type_id::list)
    type_mapping(core::numbers::decimal32, type_id::decimal32)
    type_mapping(core::numbers::decimal64, type_id::decimal64)
    type_mapping(core::numbers::decimal128, type_id::decimal128)
    type_mapping(structs::struct_view, type_id::structs)
    // clang-format on

#undef type_mapping

                                                                                template<type_id Id>
                                                                                struct dispatch_storage_type {
        using type = device_storage_type_t<typename id_to_type_impl<Id>::type>;
    };

    template<typename T>
    struct type_to_scalar_type_impl {
        using scalar_type = scalar::scalar_t;
    };

#ifndef map_numeric_scalar
#define map_numeric_scalar(Type)                                                                                       \
    template<>                                                                                                         \
    struct type_to_scalar_type_impl<Type> {                                                                            \
        using scalar_type = scalar::numeric_scalar<Type>;                                                              \
    };
#endif
    // clang-format off
    map_numeric_scalar(int8_t)
    map_numeric_scalar(int16_t)
    map_numeric_scalar(int32_t)
    map_numeric_scalar(int64_t)
    map_numeric_scalar(absl::int128)
    map_numeric_scalar(uint8_t)
    map_numeric_scalar(uint16_t)
    map_numeric_scalar(uint32_t)
    map_numeric_scalar(uint64_t)
    map_numeric_scalar(float)
    map_numeric_scalar(double)
    map_numeric_scalar(bool)
    // clang-format on

#undef map_numeric_scalar

                    template<>
                    struct type_to_scalar_type_impl<std::string> {
        using scalar_type = scalar::string_scalar;
    };

    template<>
    struct type_to_scalar_type_impl<std::string_view> {
        using scalar_type = scalar::string_scalar;
    };

    template<>
    struct type_to_scalar_type_impl<core::numbers::decimal32> {
        using scalar_type = scalar::fixed_point_scalar<core::numbers::decimal32>;
    };

    template<>
    struct type_to_scalar_type_impl<core::numbers::decimal64> {
        using scalar_type = scalar::fixed_point_scalar<core::numbers::decimal64>;
    };

    template<>
    struct type_to_scalar_type_impl<core::numbers::decimal128> {
        using scalar_type = scalar::fixed_point_scalar<core::numbers::decimal128>;
    };

    template<>
    struct type_to_scalar_type_impl<dictionary::dictionary32> {
        using scalar_type = scalar::numeric_scalar<int32_t>;
    };

    template<>
    struct type_to_scalar_type_impl<lists::list_view> {
        using scalar_type = scalar::list_scalar;
    };

    template<>
    struct type_to_scalar_type_impl<structs::struct_view> {
        using scalar_type = scalar::struct_scalar;
    };

#ifndef map_timestamp_scalar
#define map_timestamp_scalar(Type)                                                                                     \
    template<>                                                                                                         \
    struct type_to_scalar_type_impl<Type> {                                                                            \
        using scalar_type = scalar::timestamp_scalar<Type>;                                                            \
    };
#endif
    // clang-format off
    map_timestamp_scalar(core::date::timestamp_day)
    map_timestamp_scalar(core::date::timestamp_s)
    map_timestamp_scalar(core::date::timestamp_ms)
    map_timestamp_scalar(core::date::timestamp_us)
    map_timestamp_scalar(core::date::timestamp_ns)
    // clang-format on

#undef map_timestamp_scalar

#ifndef map_duration_scalar
#define map_duration_scalar(Type)                                                                                      \
    template<>                                                                                                         \
    struct type_to_scalar_type_impl<Type> {                                                                            \
        using scalar_type = scalar::duration_scalar<Type>;                                                             \
    };
#endif
        // clang-format off
    map_duration_scalar(core::date::duration_day)
    map_duration_scalar(core::date::duration_s)
    map_duration_scalar(core::date::duration_ms)
    map_duration_scalar(core::date::duration_us)
    map_duration_scalar(core::date::duration_ns)
    // clang-format on

#undef map_duration_scalar

                    template<typename T>
                    using scalar_type_t = typename type_to_scalar_type_impl<T>::scalar_type;

    template<typename T>
    using scalar_device_type_t = typename type_to_scalar_type_impl<T>::ScalarDeviceType;

    template<template<type_id> typename idtypemap = id_to_type_impl, typename functor, typename... ts>
    constexpr decltype(auto) type_dispatcher(data_type dtype, functor f, ts&&... args) {
        switch (dtype.id()) {
            case type_id::bool8:
                return f.template operator()<typename idtypemap<type_id::bool8>::type>(std::forward<ts>(args)...);
            case type_id::int8:
                return f.template operator()<typename idtypemap<type_id::int8>::type>(std::forward<ts>(args)...);
            case type_id::int16:
                return f.template operator()<typename idtypemap<type_id::int16>::type>(std::forward<ts>(args)...);
            case type_id::int32:
                return f.template operator()<typename idtypemap<type_id::int32>::type>(std::forward<ts>(args)...);
            case type_id::int64:
                return f.template operator()<typename idtypemap<type_id::int64>::type>(std::forward<ts>(args)...);
            case type_id::uint8:
                return f.template operator()<typename idtypemap<type_id::uint8>::type>(std::forward<ts>(args)...);
            case type_id::uint16:
                return f.template operator()<typename idtypemap<type_id::uint16>::type>(std::forward<ts>(args)...);
            case type_id::uint32:
                return f.template operator()<typename idtypemap<type_id::uint32>::type>(std::forward<ts>(args)...);
            case type_id::uint64:
                return f.template operator()<typename idtypemap<type_id::uint64>::type>(std::forward<ts>(args)...);
            case type_id::float32:
                return f.template operator()<typename idtypemap<type_id::float32>::type>(std::forward<ts>(args)...);
            case type_id::float64:
                return f.template operator()<typename idtypemap<type_id::float64>::type>(std::forward<ts>(args)...);
            case type_id::string:
                return f.template operator()<typename idtypemap<type_id::string>::type>(std::forward<ts>(args)...);
            case type_id::timestamp_days:
                return f.template operator()<typename idtypemap<type_id::timestamp_days>::type>(
                    std::forward<ts>(args)...);
            case type_id::timestamp_seconds:
                return f.template operator()<typename idtypemap<type_id::timestamp_seconds>::type>(
                    std::forward<ts>(args)...);
            case type_id::timestamp_milliseconds:
                return f.template operator()<typename idtypemap<type_id::timestamp_milliseconds>::type>(
                    std::forward<ts>(args)...);
            case type_id::timestamp_microseconds:
                return f.template operator()<typename idtypemap<type_id::timestamp_microseconds>::type>(
                    std::forward<ts>(args)...);
            case type_id::timestamp_nanoseconds:
                return f.template operator()<typename idtypemap<type_id::timestamp_nanoseconds>::type>(
                    std::forward<ts>(args)...);
            case type_id::duration_days:
                return f.template operator()<typename idtypemap<type_id::duration_days>::type>(
                    std::forward<ts>(args)...);
            case type_id::duration_seconds:
                return f.template operator()<typename idtypemap<type_id::duration_seconds>::type>(
                    std::forward<ts>(args)...);
            case type_id::duration_milliseconds:
                return f.template operator()<typename idtypemap<type_id::duration_milliseconds>::type>(
                    std::forward<ts>(args)...);
            case type_id::duration_microseconds:
                return f.template operator()<typename idtypemap<type_id::duration_microseconds>::type>(
                    std::forward<ts>(args)...);
            case type_id::duration_nanoseconds:
                return f.template operator()<typename idtypemap<type_id::duration_nanoseconds>::type>(
                    std::forward<ts>(args)...);
            case type_id::dictionary32:
                return f.template operator()<typename idtypemap<type_id::dictionary32>::type>(
                    std::forward<ts>(args)...);
            case type_id::list:
                return f.template operator()<typename idtypemap<type_id::list>::type>(std::forward<ts>(args)...);
            case type_id::decimal32:
                return f.template operator()<typename idtypemap<type_id::decimal32>::type>(std::forward<ts>(args)...);
            case type_id::decimal64:
                return f.template operator()<typename idtypemap<type_id::decimal64>::type>(std::forward<ts>(args)...);
            case type_id::decimal128:
                return f.template operator()<typename idtypemap<type_id::decimal128>::type>(std::forward<ts>(args)...);
            case type_id::structs:
                return f.template operator()<typename idtypemap<type_id::structs>::type>(std::forward<ts>(args)...);
            default: {
                assert(false);
            }
        }
    }

    namespace detail {
        template<typename T1>
        struct double_type_dispatcher_second_type {
            template<typename T2, typename F, typename... Ts>
            decltype(auto) operator()(F&& f, Ts&&... args) const {
                return f.template operator()<T1, T2>(std::forward<Ts>(args)...);
            }
        };

        template<template<type_id> typename IdTypeMap>
        struct double_type_dispatcher_first_type {
            template<typename T1, typename F, typename... Ts>
            decltype(auto) operator()(data_type type2, F&& f, Ts&&... args) const {
                return type_dispatcher<IdTypeMap>(type2,
                                                  detail::double_type_dispatcher_second_type<T1>{},
                                                  std::forward<F>(f),
                                                  std::forward<Ts>(args)...);
            }
        };
    } // namespace detail

    template<template<type_id> typename IdTypeMap = id_to_type_impl, typename F, typename... Ts>
    constexpr decltype(auto) double_type_dispatcher(data_type type1, data_type type2, F&& f, Ts&&... args) {
        return type_dispatcher<IdTypeMap>(type1,
                                          detail::double_type_dispatcher_first_type<IdTypeMap>{},
                                          type2,
                                          std::forward<F>(f),
                                          std::forward<Ts>(args)...);
    }

} // namespace components::dataframe
