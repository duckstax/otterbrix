#include "cast.hpp"

#include <core/assert/assert.hpp>
#include <dataframe/column/column.hpp>
#include <dataframe/detail/bitmask.hpp>
#include <dataframe/traits.hpp>
#include <utility>

namespace {

    using namespace components::dataframe;

    template<typename From, typename To>
    constexpr inline auto is_supported_non_fixed_point_cast() {
        return is_fixed_width<To>() &&
               !(is_fixed_point<From>() || is_fixed_point<To>()) &&
               !(is_timestamp<From>() && is_numeric<To>()) &&
               !(is_timestamp<To>() && is_numeric<From>());
    }

    template<typename From, typename To>
    constexpr inline auto is_supported_fixed_point_cast() {
        return (is_fixed_point<From>() && is_numeric<To>()) ||
               (is_numeric<From>() && is_fixed_point<To>()) ||
               (is_fixed_point<From>() && is_fixed_point<To>());
    }

    template<typename From, typename To>
    constexpr inline auto is_supported_cast() {
        return is_supported_non_fixed_point_cast<From, To>() || is_supported_fixed_point_cast<From, To>();
    }

    template<typename To, typename From>
    constexpr inline To unary_cast(const From element) {
        if constexpr (is_numeric<From>() && is_numeric<To>()) {
            return static_cast<To>(element);
        } else if constexpr (is_timestamp<From>() && is_timestamp<To>()) {
            return To{std::chrono::floor<To::duration>(element.time_since_epoch())};
        } else if constexpr (is_duration<From>() && is_duration<To>()) {
            return To{std::chrono::floor<To>(element)};
        } else if constexpr (is_numeric<From>() && is_duration<To>()) {
            return To{static_cast<typename To::rep>(element)};
        } else if constexpr (is_timestamp<From>() && is_duration<To>()) {
            return To{std::chrono::floor<To>(element.time_since_epoch())};
        } else if constexpr (is_duration<From>() && is_numeric<To>()) {
            return static_cast<To>(element.count());
        } else if constexpr (is_duration<From>() && is_timestamp<To>()) {
            return To{std::chrono::floor<To::duration>(element)};
        } else {
            assertion_exception_msg(false, "not valid unary cast");
        }
        return {};
    }

    template<typename To, typename From>
    constexpr inline To fixed_point_unary_cast(const From element, core::numbers::scale_type scale) {
        using condition = std::conditional_t<is_fixed_point<From>(), From, To>;
        using device_t = device_storage_type_t<condition>;
        if constexpr (is_fixed_point<From>() && is_numeric<To>()) {
            constexpr auto fp = From{core::numbers::scaled_integer<device_t>{element, scale}};
            return static_cast<To>(fp);
        } else if constexpr (is_numeric<From>() && is_fixed_point<To>()) {
            return To{element, scale}.value();
        } else {
            assertion_exception_msg(false, "not valid fixed point unary cast");
        }
        return {};
    }


    struct dispatch_unary_cast_from {
        column::column_view input;

        explicit dispatch_unary_cast_from(column::column_view inp)
            : input(std::move(inp)) {}

        template <typename T, std::enable_if_t<is_fixed_width<T>()>* = nullptr>
        std::unique_ptr<column::column_t> operator()(std::pmr::memory_resource* resource, data_type type) {
            return type_dispatcher(type, dispatch_unary_cast_to<T>{input}, resource, type);
        }

        template <typename T, typename... Args>
        std::enable_if_t<!is_fixed_width<T>(), std::unique_ptr<column::column_t>> operator()(Args&&...) {
            assertion_exception_msg(false, "Column type must be numeric or chrono or decimal32/64/128");
        }
    };

} // namespace

namespace components::dataframe::detail {

    std::unique_ptr<column::column_t> cast(std::pmr::memory_resource* resource, column::column_view const& input, data_type type) {
        assertion_exception_msg(is_fixed_width(type), "Unary cast type must be fixed-width.");
        return type_dispatcher(input.type(), dispatch_unary_cast_from{input}, resource, type);
    }

} // namespace components::dataframe::detail
