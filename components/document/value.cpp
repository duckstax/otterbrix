#include "value.hpp"
#include <boost/math/special_functions/factorials.hpp>
#include <cmath>

namespace components::document {

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

    value_t::value_t(impl::element element)
        : element_(element) {}

    void value_t::set(impl::element element) { element_ = element; }

    types::physical_type value_t::physical_type() const noexcept { return element_.physical_type(); }

    types::logical_type value_t::logical_type() const noexcept { return element_.logical_type(); }

    bool value_t::operator==(const value_t& rhs) const {
        switch (physical_type()) {
            case types::physical_type::BOOL:
                return as_bool() == rhs.as_bool();
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return as_unsigned() == rhs.as_unsigned();
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return as_int() == rhs.as_int();
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return as_int128() == rhs.as_int128();
            case types::physical_type::FLOAT:
                return is_equals(as_float(), rhs.as_float());
            case types::physical_type::DOUBLE:
                return is_equals(as_double(), rhs.as_double());
            case types::physical_type::STRING:
                return as_string() == rhs.as_string();
            default: // special values are always equal
                return physical_type() == rhs.physical_type();
        }
    }

    bool value_t::operator<(const value_t& rhs) const {
        switch (physical_type()) {
            case types::physical_type::BOOL:
                return as_bool() < rhs.as_bool();
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return as_unsigned() < rhs.as_unsigned();
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return as_int() < rhs.as_int();
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return as_int128() < rhs.as_int128();
            case types::physical_type::FLOAT:
                return as_float() < rhs.as_float();
            case types::physical_type::DOUBLE:
                return as_double() < rhs.as_double();
            case types::physical_type::STRING:
                return as_string() < rhs.as_string();
            default: // special values are always equal
                return false;
        }
    }

    bool value_t::as_bool() const noexcept { return element_.get_bool().value(); }

    int64_t value_t::as_int() const noexcept { return element_.get_int64().value(); }

    uint64_t value_t::as_unsigned() const noexcept { return element_.get_uint64().value(); }

    int128_t value_t::as_int128() const noexcept { return element_.get_int128().value(); }

    float value_t::as_float() const noexcept { return element_.get_float().value(); }

    double value_t::as_double() const noexcept { return element_.get_double().value(); }

    std::string_view value_t::as_string() const noexcept {
        if (physical_type() == types::physical_type::STRING) {
            return element_.get_string().value();
        } else {
            return get_string_bytes();
        }
    }

    bool value_t::is_bool() const noexcept { return physical_type() == types::physical_type::BOOL; }

    bool value_t::is_int() const noexcept {
        switch (physical_type()) {
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
            case types::physical_type::INT128:
                return true;
            default:
                return false;
        }
    }

    bool value_t::is_unsigned() const noexcept {
        switch (physical_type()) {
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
            case types::physical_type::UINT128:
                return true;
            default:
                return false;
        }
    }

    bool value_t::is_int128() const noexcept {
        return physical_type() == types::physical_type::UINT128 || physical_type() == types::physical_type::INT128;
    }

    bool value_t::is_double() const noexcept {
        return physical_type() == types::physical_type::FLOAT || physical_type() == types::physical_type::DOUBLE;
    }

    bool value_t::is_string() const noexcept { return physical_type() == types::physical_type::STRING; }

    value_t::operator bool() const { return element_.usable(); }

    const impl::element* value_t::get_element() const noexcept { return &element_; }

    template<typename, typename = void>
    constexpr bool has_operator = false;
    template<typename T>
    constexpr bool has_operator<T, std::void_t<decltype(std::declval<T>().operator())>> = true;

    template<typename OP, typename GET>
    value_t op(const value_t& value, impl::base_document* tape, GET getter_function) {
        OP operation{};
        return value_t{tape, operation((value.*getter_function)())};
    }

    template<typename OP, typename GET>
    value_t op(const value_t& value1, const value_t& value2, impl::base_document* tape, GET getter_function) {
        using T = typename std::invoke_result<decltype(getter_function), value_t>::type;
        OP operation{};
        if (!value1) {
            return value_t{tape, operation(T(), (value2.*getter_function)())};
        } else if (!value2) {
            return value_t{tape, operation((value1.*getter_function)(), T())};
        } else {
            return value_t{tape, operation((value1.*getter_function)(), (value2.*getter_function)())};
        }
    }

    value_t sum(const value_t& value1, const value_t& value2, impl::base_document* tape) {
        if (!value1 && !value2) {
            return value1;
        }

        auto type = value1 ? value1.physical_type() : value2.physical_type();
        switch (type) {
            case types::physical_type::BOOL:
                return op<std::plus<>>(value1, value2, tape, &value_t::as_bool);
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return op<std::plus<>>(value1, value2, tape, &value_t::as_unsigned);
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return op<std::plus<>>(value1, value2, tape, &value_t::as_int);
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return op<std::plus<>>(value1, value2, tape, &value_t::as_int128);
            case types::physical_type::FLOAT:
                return op<std::plus<>>(value1, value2, tape, &value_t::as_float);
            case types::physical_type::DOUBLE:
                return op<std::plus<>>(value1, value2, tape, &value_t::as_double);
            case types::physical_type::STRING:
                return value_t{tape, std::string(value1.as_string()) + std::string(value2.as_string())};
            default: // special values can't be added
                return value1;
        }
    }

    value_t subtract(const value_t& value1, const value_t& value2, impl::base_document* tape) {
        if (!value1 && !value2) {
            return value1;
        }

        auto type = value1 ? value1.physical_type() : value2.physical_type();
        switch (type) {
            case types::physical_type::BOOL:
                return op<std::minus<>>(value1, value2, tape, &value_t::as_bool);
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return op<std::minus<>>(value1, value2, tape, &value_t::as_unsigned);
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return op<std::minus<>>(value1, value2, tape, &value_t::as_int);
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return op<std::minus<>>(value1, value2, tape, &value_t::as_int128);
            case types::physical_type::FLOAT:
                return op<std::minus<>>(value1, value2, tape, &value_t::as_float);
            case types::physical_type::DOUBLE:
                return op<std::minus<>>(value1, value2, tape, &value_t::as_double);
            default: // special values and strings can't be subtructed
                return value1;
        }
    }

    value_t mult(const value_t& value1, const value_t& value2, impl::base_document* tape) {
        if (!value1 && !value2) {
            return value1;
        }

        auto type = value1 ? value1.physical_type() : value2.physical_type();
        switch (type) {
            case types::physical_type::BOOL:
                return op<std::multiplies<>>(value1, value2, tape, &value_t::as_bool);
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return op<std::multiplies<>>(value1, value2, tape, &value_t::as_unsigned);
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return op<std::multiplies<>>(value1, value2, tape, &value_t::as_int);
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return op<std::multiplies<>>(value1, value2, tape, &value_t::as_int128);
            case types::physical_type::FLOAT:
                return op<std::multiplies<>>(value1, value2, tape, &value_t::as_float);
            case types::physical_type::DOUBLE:
                return op<std::multiplies<>>(value1, value2, tape, &value_t::as_double);
            default: // special values and strings can't be multiplied
                return value1;
        }
    }

    value_t divide(const value_t& value1, const value_t& value2, impl::base_document* tape) {
        if (!value1 && !value2) {
            return value1;
        }

        // TODO: possible divide by 0 issues here
        auto type = value1 ? value1.physical_type() : value2.physical_type();
        switch (type) {
            case types::physical_type::BOOL:
                return op<std::divides<>>(value1, value2, tape, &value_t::as_bool);
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return op<std::divides<>>(value1, value2, tape, &value_t::as_unsigned);
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return op<std::divides<>>(value1, value2, tape, &value_t::as_int);
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return op<std::divides<>>(value1, value2, tape, &value_t::as_int128);
            case types::physical_type::FLOAT:
                return op<std::divides<>>(value1, value2, tape, &value_t::as_float);
            case types::physical_type::DOUBLE:
                return op<std::divides<>>(value1, value2, tape, &value_t::as_double);
            default: // special values and strings can't be divided
                return value1;
        }
    }

    value_t modulus(const value_t& value1, const value_t& value2, impl::base_document* tape) {
        if (!value1 && !value2) {
            return value1;
        }

        auto type = value1 ? value1.physical_type() : value2.physical_type();
        switch (type) {
            case types::physical_type::BOOL:
                return op<std::modulus<>>(value1, value2, tape, &value_t::as_bool);
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return op<std::modulus<>>(value1, value2, tape, &value_t::as_unsigned);
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return op<std::modulus<>>(value1, value2, tape, &value_t::as_int);
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return op<std::modulus<>>(value1, value2, tape, &value_t::as_int128);
            default: // special values, strings, floats and double can't be modulated
                return value1;
        }
    }

    value_t exponent(const value_t& value1, const value_t& value2, impl::base_document* tape) {
        if (!value1 && !value2) {
            return value1;
        }

        auto type = value1 ? value1.physical_type() : value2.physical_type();
        switch (type) {
            case types::physical_type::BOOL:
                return op<pow<>>(value1, value2, tape, &value_t::as_bool);
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return op<pow<>>(value1, value2, tape, &value_t::as_unsigned);
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return op<pow<>>(value1, value2, tape, &value_t::as_int);
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return op<pow<>>(value1, value2, tape, &value_t::as_int128);
            case types::physical_type::FLOAT:
                return op<pow<>>(value1, value2, tape, &value_t::as_float);
            case types::physical_type::DOUBLE:
                return op<pow<>>(value1, value2, tape, &value_t::as_double);
            default: // special values and strings can't be multiplied
                return value1;
        }
    }

    value_t sqr_root(const value_t& value, impl::base_document* tape) {
        if (!value) {
            return value;
        }

        switch (value.physical_type()) {
            case types::physical_type::BOOL:
                return op<sqrt<>>(value, tape, &value_t::as_bool);
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return op<sqrt<>>(value, tape, &value_t::as_unsigned);
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return op<sqrt<>>(value, tape, &value_t::as_int);
            case types::physical_type::FLOAT:
                return op<sqrt<>>(value, tape, &value_t::as_float);
            case types::physical_type::DOUBLE:
                return op<sqrt<>>(value, tape, &value_t::as_double);
            default: // special values and strings can't be multiplied; int128 is undefined
                return value;
        }
    }

    value_t cube_root(const value_t& value, impl::base_document* tape) {
        if (!value) {
            return value;
        }

        switch (value.physical_type()) {
            case types::physical_type::BOOL:
                return op<cbrt<>>(value, tape, &value_t::as_bool);
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return op<cbrt<>>(value, tape, &value_t::as_unsigned);
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return op<cbrt<>>(value, tape, &value_t::as_int);
            case types::physical_type::FLOAT:
                return op<cbrt<>>(value, tape, &value_t::as_float);
            case types::physical_type::DOUBLE:
                return op<cbrt<>>(value, tape, &value_t::as_double);
            default: // special values and strings can't be multiplied; int128 is undefined
                return value;
        }
    }

    value_t factorial(const value_t& value, impl::base_document* tape) {
        if (!value) {
            return value;
        }

        switch (value.physical_type()) {
            case types::physical_type::BOOL:
                return op<fact<>>(value, tape, &value_t::as_bool);
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return op<fact<>>(value, tape, &value_t::as_unsigned);
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return op<fact<>>(value, tape, &value_t::as_int);
            case types::physical_type::FLOAT:
                return op<fact<>>(value, tape, &value_t::as_float);
            case types::physical_type::DOUBLE:
                return op<fact<>>(value, tape, &value_t::as_double);
            default: // special values and strings can't be multiplied; int128 is undefined
                return value;
        }
    }

    value_t absolute(const value_t& value, impl::base_document* tape) {
        if (!value) {
            return value;
        }

        switch (value.physical_type()) {
            case types::physical_type::BOOL:
                return op<abs<>>(value, tape, &value_t::as_bool);
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                //special for int128
                return value;
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return op<abs<>>(value, tape, &value_t::as_int);
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return op<abs<>>(value, tape, &value_t::as_int128);
            case types::physical_type::FLOAT:
                return op<abs<>>(value, tape, &value_t::as_float);
            case types::physical_type::DOUBLE:
                return op<abs<>>(value, tape, &value_t::as_double);
            default: // special values and strings can't be multiplied
                return value;
        }
    }

    value_t bit_and(const value_t& value1, const value_t& value2, impl::base_document* tape) {
        if (!value1 && !value2) {
            return value1;
        }

        auto type = value1 ? value1.physical_type() : value2.physical_type();
        switch (type) {
            case types::physical_type::BOOL:
                return op<std::bit_and<>>(value1, value2, tape, &value_t::as_bool);
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return op<std::bit_and<>>(value1, value2, tape, &value_t::as_unsigned);
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return op<std::bit_and<>>(value1, value2, tape, &value_t::as_int);
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return op<std::bit_and<>>(value1, value2, tape, &value_t::as_int128);
            default: // special values, floats and strings can't bitwised
                return value1;
        }
    }

    value_t bit_or(const value_t& value1, const value_t& value2, impl::base_document* tape) {
        if (!value1 && !value2) {
            return value1;
        }

        auto type = value1 ? value1.physical_type() : value2.physical_type();
        switch (type) {
            case types::physical_type::BOOL:
                return op<std::bit_or<>>(value1, value2, tape, &value_t::as_bool);
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return op<std::bit_or<>>(value1, value2, tape, &value_t::as_unsigned);
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return op<std::bit_or<>>(value1, value2, tape, &value_t::as_int);
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return op<std::bit_or<>>(value1, value2, tape, &value_t::as_int128);
            default: // special values, floats and strings can't bitwised
                return value1;
        }
    }

    value_t bit_xor(const value_t& value1, const value_t& value2, impl::base_document* tape) {
        if (!value1 && !value2) {
            return value1;
        }

        auto type = value1 ? value1.physical_type() : value2.physical_type();
        switch (type) {
            case types::physical_type::BOOL:
                return op<std::bit_xor<>>(value1, value2, tape, &value_t::as_bool);
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return op<std::bit_xor<>>(value1, value2, tape, &value_t::as_unsigned);
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return op<std::bit_xor<>>(value1, value2, tape, &value_t::as_int);
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return op<std::bit_xor<>>(value1, value2, tape, &value_t::as_int128);
            default: // special values, floats and strings can't bitwised
                return value1;
        }
    }

    value_t bit_not(const value_t& value, impl::base_document* tape) {
        if (!value) {
            return value;
        }

        switch (value.physical_type()) {
            case types::physical_type::BOOL:
                return op<std::bit_not<>>(value, tape, &value_t::as_bool);
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return op<std::bit_not<>>(value, tape, &value_t::as_unsigned);
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return op<std::bit_not<>>(value, tape, &value_t::as_int);
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return op<std::bit_not<>>(value, tape, &value_t::as_int128);
            default: // special values, floats and strings can't bitwised
                return value;
        }
    }

    value_t bit_shift_l(const value_t& value1, const value_t& value2, impl::base_document* tape) {
        if (!value1 && !value2) {
            return value1;
        }

        auto type = value1 ? value1.physical_type() : value2.physical_type();
        switch (type) {
            case types::physical_type::BOOL:
                return op<shift_left<>>(value1, value2, tape, &value_t::as_bool);
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return op<shift_left<>>(value1, value2, tape, &value_t::as_unsigned);
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return op<shift_left<>>(value1, value2, tape, &value_t::as_int);
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return op<shift_left<>>(value1, value2, tape, &value_t::as_int128);
            default: // special values and strings can't bitwised, float can't be shifted
                return value1;
        }
    }

    value_t bit_shift_r(const value_t& value1, const value_t& value2, impl::base_document* tape) {
        if (!value1 && !value2) {
            return value1;
        }

        auto type = value1 ? value1.physical_type() : value2.physical_type();
        switch (type) {
            case types::physical_type::BOOL:
                return op<shift_right<>>(value1, value2, tape, &value_t::as_bool);
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return op<shift_right<>>(value1, value2, tape, &value_t::as_unsigned);
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return op<shift_right<>>(value1, value2, tape, &value_t::as_int);
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return op<shift_right<>>(value1, value2, tape, &value_t::as_int128);
            default: // special values and strings can't bitwised, float can't be shifted
                return value1;
        }
    }

    std::string_view value_t::get_string_bytes() const noexcept {
        char* res = new char[4];
        if (is_int()) {
            auto temp = as_int();
            memcpy(res, &temp, 4);
        } else if (is_unsigned()) {
            auto temp = as_unsigned();
            memcpy(res, &temp, 4);
        } else if (is_double()) {
            auto temp = as_double();
            memcpy(res, &temp, 4);
        } else {
            memset(res, 0, 4);
        }
        return std::string_view(res, 4);
    }
} // namespace components::document