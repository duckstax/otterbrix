#pragma once

#include <limits>

#include "dataframe/types.hpp"

namespace core {

    template<typename Type>
    struct dictionary_wrapper {
        using value_type = Type;

        dictionary_wrapper() = default;
        ~dictionary_wrapper() = default;
        dictionary_wrapper(dictionary_wrapper&&) = default;
        dictionary_wrapper(dictionary_wrapper const&) = default;
        dictionary_wrapper& operator=(dictionary_wrapper&&) = default;
        dictionary_wrapper& operator=(const dictionary_wrapper&) = default;

        inline constexpr explicit dictionary_wrapper(value_type v)
            : _value{v} {}

        inline explicit operator value_type() const { return _value; }

        inline value_type value() const { return _value; }

        static inline constexpr value_type max_value() { return std::numeric_limits<value_type>::max(); }

        static inline constexpr value_type min_value() { return std::numeric_limits<value_type>::min(); }

        static inline constexpr value_type lowest_value() { return std::numeric_limits<value_type>::lowest(); }

    private:
        value_type _value;
    };

    template<typename Integer>
    inline bool operator==(dictionary_wrapper<Integer> const& lhs, dictionary_wrapper<Integer> const& rhs) {
        return lhs.value() == rhs.value();
    }

    template<typename Integer>
    inline bool operator!=(dictionary_wrapper<Integer> const& lhs, dictionary_wrapper<Integer> const& rhs) {
        return lhs.value() != rhs.value();
    }

    template<typename Integer>
    inline bool operator<=(dictionary_wrapper<Integer> const& lhs, dictionary_wrapper<Integer> const& rhs) {
        return lhs.value() <= rhs.value();
    }

    template<typename Integer>
    inline bool operator>=(dictionary_wrapper<Integer> const& lhs, dictionary_wrapper<Integer> const& rhs) {
        return lhs.value() >= rhs.value();
    }

    template<typename Integer>
    inline constexpr bool operator<(dictionary_wrapper<Integer> const& lhs, dictionary_wrapper<Integer> const& rhs) {
        return lhs.value() < rhs.value();
    }

    template<typename Integer>
    inline bool operator>(dictionary_wrapper<Integer> const& lhs, dictionary_wrapper<Integer> const& rhs) {
        return lhs.value() > rhs.value();
    }

    using dictionary32 = dictionary_wrapper<int32_t>;

} // namespace core
