#pragma once

#include <cmath>
#include <cstdlib>
#include <stdexcept>
#include <type_traits>

#include "traits.hpp"

namespace components::dataframe::math {

    template<typename S, typename T>
    constexpr S div_rounding_up_unsafe(const S& dividend, const T& divisor) noexcept {
        return (dividend + divisor - 1) / divisor;
    }

    namespace detail {
        template<typename I>
        constexpr I div_rounding_up_safe(std::integral_constant<bool, false>, I dividend, I divisor) noexcept {
            return (dividend > divisor) ? 1 + div_rounding_up_unsafe(dividend - divisor, divisor) : (dividend > 0);
        }

        template<typename I>
        constexpr I div_rounding_up_safe(std::integral_constant<bool, true>, I dividend, I divisor) noexcept {
            auto quotient = dividend / divisor;
            auto remainder = dividend % divisor;
            return quotient + (remainder != 0);
        }

    } // namespace detail

    template<typename I>
    constexpr I div_rounding_up_safe(I dividend, I divisor) noexcept {
        using i_is_a_signed_type = std::integral_constant<bool, std::is_signed_v<I>>;
        return detail::div_rounding_up_safe(i_is_a_signed_type{}, dividend, divisor);
    }

} // namespace components::dataframe::math
