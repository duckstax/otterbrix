#include "physical_value.hpp"

#include <array>

template<class T, class U>
constexpr bool cmp_less(T t, U u) noexcept {
    if constexpr (std::is_floating_point_v<T> && std::is_floating_point_v<U>) {
        return t < u;
    } else if constexpr (std::is_floating_point_v<T> && !std::is_floating_point_v<U>) {
        return t < T(u);
    } else if constexpr (!std::is_floating_point_v<T> && std::is_floating_point_v<U>) {
        return U(t) < u;
    } else if constexpr (std::is_signed_v<T> == std::is_signed_v<U>) {
        return t < u;
    } else if constexpr (std::is_signed_v<T>) {
        return t < 0 || std::make_unsigned_t<T>(t) < u;
    } else {
        return u >= 0 && t < std::make_unsigned_t<U>(u);
    }
}

namespace components::types {

    constexpr bool operator<(physical_type lhs, physical_type rhs) noexcept {
        return static_cast<uint8_t>(lhs) < static_cast<uint8_t>(rhs);
    }
    constexpr bool operator>(physical_type lhs, physical_type rhs) noexcept {
        return static_cast<uint8_t>(lhs) > static_cast<uint8_t>(rhs);
    }
    constexpr bool operator==(physical_type lhs, physical_type rhs) noexcept {
        return static_cast<uint8_t>(lhs) == static_cast<uint8_t>(rhs);
    }
    constexpr bool operator!=(physical_type lhs, physical_type rhs) noexcept {
        return static_cast<uint8_t>(lhs) != static_cast<uint8_t>(rhs);
    }
    constexpr bool operator<=(physical_type lhs, physical_type rhs) noexcept {
        return static_cast<uint8_t>(lhs) <= static_cast<uint8_t>(rhs);
    }
    constexpr bool operator>=(physical_type lhs, physical_type rhs) noexcept {
        return static_cast<uint8_t>(lhs) >= static_cast<uint8_t>(rhs);
    }

    bool is_integral_type(const physical_type type) noexcept {
        if (type >= physical_type::UINT8 && type <= physical_type::DOUBLE) {
            return true;
        } else {
            return false;
        }
    }

    physical_value::physical_value() {}

    physical_value::physical_value(bool value) {
        data_ = new uint8_t[1];
        *data_ = static_cast<uint8_t>(value ? physical_type::BOOL_TRUE : physical_type::BOOL_FALSE);
    }

    physical_value::physical_value(uint8_t value) {
        data_ = new uint8_t[sizeof(value) + 1];
        *data_ = static_cast<uint8_t>(physical_type::UINT8);
        *(data_ + 1) = value;
    }

    physical_value::physical_value(uint16_t value) {
        data_ = new uint8_t[sizeof(value) + 1];
        *data_ = static_cast<uint8_t>(physical_type::UINT16);
        *reinterpret_cast<uint16_t*>(data_ + 1) = value;
    }

    physical_value::physical_value(uint32_t value) {
        data_ = new uint8_t[sizeof(value) + 1];
        *data_ = static_cast<uint8_t>(physical_type::UINT32);
        *reinterpret_cast<uint32_t*>(data_ + 1) = value;
    }

    physical_value::physical_value(uint64_t value) {
        data_ = new uint8_t[sizeof(value) + 1];
        *data_ = static_cast<uint8_t>(physical_type::UINT64);
        *reinterpret_cast<uint64_t*>(data_ + 1) = value;
    }

    physical_value::physical_value(int8_t value) {
        data_ = new uint8_t[sizeof(value) + 1];
        *data_ = static_cast<uint8_t>(physical_type::INT8);
        *reinterpret_cast<int8_t*>(data_ + 1) = value;
    }

    physical_value::physical_value(int16_t value) {
        data_ = new uint8_t[sizeof(value) + 1];
        *data_ = static_cast<uint8_t>(physical_type::INT16);
        *reinterpret_cast<int16_t*>(data_ + 1) = value;
    }

    physical_value::physical_value(int32_t value) {
        data_ = new uint8_t[sizeof(value) + 1];
        *data_ = static_cast<uint8_t>(physical_type::INT32);
        *reinterpret_cast<int32_t*>(data_ + 1) = value;
    }

    physical_value::physical_value(int64_t value) {
        data_ = new uint8_t[sizeof(value) + 1];
        *data_ = static_cast<uint8_t>(physical_type::INT64);
        *reinterpret_cast<int64_t*>(data_ + 1) = value;
    }

    physical_value::physical_value(float value) {
        data_ = new uint8_t[sizeof(value) + 1];
        *data_ = static_cast<uint8_t>(physical_type::FLOAT);
        *reinterpret_cast<float*>(data_ + 1) = value;
    }

    physical_value::physical_value(double value) {
        data_ = new uint8_t[sizeof(value) + 1];
        *data_ = static_cast<uint8_t>(physical_type::DOUBLE);
        *reinterpret_cast<double*>(data_ + 1) = value;
    }

    physical_value::physical_value(const std::string& value) {
        data_ = new uint8_t[value.size() + sizeof(uint32_t) + 1];
        *data_ = static_cast<uint8_t>(physical_type::STRING);
        *reinterpret_cast<uint32_t*>(data_ + 1) = value.size();
        std::memcpy(data_ + 1 + sizeof(uint32_t), value.data(), value.size());
    }

    physical_value::physical_value(const physical_value& other) {
        if (!other.data_) {
            return;
        }

        switch (static_cast<physical_type>(*other.data_)) {
            case physical_type::BOOL_FALSE:
            case physical_type::BOOL_TRUE:
                data_ = new uint8_t(*other.data_);
                break;
            case physical_type::UINT8:
            case physical_type::INT8:
                data_ = new uint8_t[2];
                std::memcpy(data_, other.data_, 2);
                break;
            case physical_type::UINT16:
            case physical_type::INT16:
                data_ = new uint8_t[3];
                std::memcpy(data_, other.data_, 3);
                break;
            case physical_type::UINT32:
            case physical_type::INT32:
            case physical_type::FLOAT:
                data_ = new uint8_t[5];
                std::memcpy(data_, other.data_, 5);
                break;
            case physical_type::UINT64:
            case physical_type::INT64:
            case physical_type::DOUBLE:
                data_ = new uint8_t[9];
                std::memcpy(data_, other.data_, 9);
                break;
            case physical_type::UINT128:
            case physical_type::INT128:
                assert(false && "int128 is not supported yet");
                break;
            case physical_type::STRING: {
                uint32_t size = *reinterpret_cast<uint32_t*>(other.data_ + 1) + sizeof(uint32_t) + 1;
                data_ = new uint8_t[size];
                std::memcpy(data_, other.data_, size);
                break;
            }
            default:
                break;
        }
    }

    physical_value::physical_value(physical_value&& other) noexcept {
        data_ = other.data_;
        other.data_ = nullptr;
    }

    physical_value& physical_value::operator=(const physical_value& other) {
        if (*this == other) {
            return *this;
        }

        delete[] data_;

        if (!other.data_) {
            return *this;
        }

        switch (static_cast<physical_type>(*other.data_)) {
            case physical_type::BOOL_FALSE:
            case physical_type::BOOL_TRUE:
                data_ = new uint8_t(*other.data_);
                break;
            case physical_type::UINT8:
            case physical_type::INT8:
                data_ = new uint8_t[2];
                std::memcpy(data_, other.data_, 2);
                break;
            case physical_type::UINT16:
            case physical_type::INT16:
                data_ = new uint8_t[3];
                std::memcpy(data_, other.data_, 3);
                break;
            case physical_type::UINT32:
            case physical_type::INT32:
            case physical_type::FLOAT:
                data_ = new uint8_t[5];
                std::memcpy(data_, other.data_, 5);
                break;
            case physical_type::UINT64:
            case physical_type::INT64:
            case physical_type::DOUBLE:
                data_ = new uint8_t[9];
                std::memcpy(data_, other.data_, 9);
                break;
            case physical_type::UINT128:
            case physical_type::INT128:
                assert(false && "int128 is not supported yet");
                break;
            case physical_type::STRING: {
                uint32_t size = *reinterpret_cast<uint32_t*>(other.data_ + 1) + sizeof(uint32_t) + 1;
                data_ = new uint8_t[size];
                std::memcpy(data_, other.data_, size);
                break;
            }
            default:
                break;
        }

        return *this;
    }

    physical_value& physical_value::operator=(physical_value&& other) noexcept {
        if (*this == other) {
            return *this;
        }

        delete[] data_;

        data_ = other.data_;
        other.data_ = nullptr;
        return *this;
    }

    physical_value::~physical_value() { delete[] data_; }

    bool physical_value::operator<(const physical_value& other) const noexcept {
        // types order:
        // 1) booleans
        // 2) numbers (including floating points) by value
        // 3) strings
        // 4) nulls

        // comparing types and values is a mess, but does a job

        // handle nulls
        if (!data_ && other.data_) {
            return false;
        }
        if (data_ && !other.data_) {
            return true;
        }
        if (!data_ && !other.data_) {
            return false;
        }

        const physical_type my_type = static_cast<physical_type>(*data_);
        const physical_type other_type = static_cast<physical_type>(*other.data_);

        assert(!(my_type == physical_type::INT128 || my_type == physical_type::UINT128) &&
               "physical value does not support int128 for now");
        assert(!(other_type == physical_type::INT128 || other_type == physical_type::UINT128) &&
               "physical value does not support int128 for now");

        // handle strings

        if (my_type == physical_type::STRING && other_type == physical_type::STRING) {
            return std::string(reinterpret_cast<char*>(data_ + sizeof(uint32_t) + 1),
                               *reinterpret_cast<uint32_t*>(data_ + 1)) <
                   std::string(reinterpret_cast<char*>(other.data_ + sizeof(uint32_t) + 1),
                               *reinterpret_cast<uint32_t*>(other.data_ + 1));
        }

        // handle booleans and non-comparable types

        if (!is_integral_type(my_type) || !is_integral_type(other_type)) {
            return my_type < other_type;
        }

        // idealy use something like this
        // return value<my_type>() < value<other_type>();
        // but type is not a constexpr, so here is a huge switch:

#define OTHER_SWITCH                                                                                                   \
    switch (other_type) {                                                                                              \
        case physical_type::UINT8:                                                                                     \
            return cmp_less(my_value, other.value<physical_type::UINT8>());                                            \
        case physical_type::INT8:                                                                                      \
            return cmp_less(my_value, other.value<physical_type::INT8>());                                             \
        case physical_type::UINT16:                                                                                    \
            return cmp_less(my_value, other.value<physical_type::UINT16>());                                           \
        case physical_type::INT16:                                                                                     \
            return cmp_less(my_value, other.value<physical_type::INT16>());                                            \
        case physical_type::UINT32:                                                                                    \
            return cmp_less(my_value, other.value<physical_type::UINT32>());                                           \
        case physical_type::INT32:                                                                                     \
            return cmp_less(my_value, other.value<physical_type::INT32>());                                            \
        case physical_type::UINT64:                                                                                    \
            return cmp_less(my_value, other.value<physical_type::UINT64>());                                           \
        case physical_type::INT64:                                                                                     \
            return cmp_less(my_value, other.value<physical_type::INT64>());                                            \
        case physical_type::FLOAT:                                                                                     \
            return cmp_less(my_value, other.value<physical_type::FLOAT>());                                            \
        case physical_type::DOUBLE:                                                                                    \
            return cmp_less(my_value, other.value<physical_type::DOUBLE>());                                           \
        default:                                                                                                       \
            assert(false && "incorrect type");                                                                         \
            return false;                                                                                              \
    }

        switch (my_type) {
            case physical_type::UINT8: {
                auto my_value = value<physical_type::UINT8>();
                OTHER_SWITCH
            }
            case physical_type::INT8: {
                auto my_value = value<physical_type::INT8>();
                OTHER_SWITCH
            }
            case physical_type::UINT16: {
                auto my_value = value<physical_type::UINT16>();
                OTHER_SWITCH
            }
            case physical_type::INT16: {
                auto my_value = value<physical_type::INT16>();
                OTHER_SWITCH
            }
            case physical_type::UINT32: {
                auto my_value = value<physical_type::UINT32>();
                OTHER_SWITCH
            }
            case physical_type::INT32: {
                auto my_value = value<physical_type::INT32>();
                OTHER_SWITCH
            }
            case physical_type::UINT64: {
                auto my_value = value<physical_type::UINT64>();
                OTHER_SWITCH
            }
            case physical_type::INT64: {
                auto my_value = value<physical_type::INT64>();
                OTHER_SWITCH
            }
            case physical_type::FLOAT: {
                auto my_value = value<physical_type::FLOAT>();
                OTHER_SWITCH
            }
            case physical_type::DOUBLE: {
                auto my_value = value<physical_type::DOUBLE>();
                OTHER_SWITCH
            }
            default:
                assert(false && "incorrect type");
                return false;
        }
    }

    bool physical_value::operator>(const physical_value& other) const noexcept { return other < *this; }

    bool physical_value::operator==(const physical_value& other) const noexcept {
        return !(*this < other) && !(*this > other);
    }

    bool physical_value::operator!=(const physical_value& other) const noexcept { return !(*this == other); }

    bool physical_value::operator<=(const physical_value& other) const noexcept { return !(*this > other); }

    bool physical_value::operator>=(const physical_value& other) const noexcept { return !(*this < other); }

    physical_type physical_value::type() const noexcept {
        if (!data_) {
            return physical_type::NA;
        } else {
            return static_cast<physical_type>(*data_);
        }
    }

    nullptr_t physical_value::value(std::integral_constant<physical_type, physical_type::NA>) const noexcept {
        assert(!data_);
        return nullptr;
    }

    bool physical_value::value(std::integral_constant<physical_type, physical_type::BOOL_FALSE>) const noexcept {
        assert(static_cast<physical_type>(*data_) == physical_type::BOOL_FALSE);
        return false;
    }

    bool physical_value::value(std::integral_constant<physical_type, physical_type::BOOL_TRUE>) const noexcept {
        assert(static_cast<physical_type>(*data_) == physical_type::BOOL_TRUE);
        return true;
    }

    uint8_t physical_value::value(std::integral_constant<physical_type, physical_type::UINT8>) const noexcept {
        assert(static_cast<physical_type>(*data_) == physical_type::UINT8);
        return *(data_ + 1);
    }

    uint16_t physical_value::value(std::integral_constant<physical_type, physical_type::UINT16>) const noexcept {
        assert(static_cast<physical_type>(*data_) == physical_type::UINT16);
        return *reinterpret_cast<uint16_t*>(data_ + 1);
    }

    uint32_t physical_value::value(std::integral_constant<physical_type, physical_type::UINT32>) const noexcept {
        assert(static_cast<physical_type>(*data_) == physical_type::UINT32);
        return *reinterpret_cast<uint32_t*>(data_ + 1);
    }

    uint64_t physical_value::value(std::integral_constant<physical_type, physical_type::UINT64>) const noexcept {
        assert(static_cast<physical_type>(*data_) == physical_type::UINT64);
        return *reinterpret_cast<uint64_t*>(data_ + 1);
    }

    int8_t physical_value::value(std::integral_constant<physical_type, physical_type::INT8>) const noexcept {
        assert(static_cast<physical_type>(*data_) == physical_type::INT8);
        return *reinterpret_cast<int8_t*>(data_ + 1);
    }

    int16_t physical_value::value(std::integral_constant<physical_type, physical_type::INT16>) const noexcept {
        assert(static_cast<physical_type>(*data_) == physical_type::INT16);
        return *reinterpret_cast<int16_t*>(data_ + 1);
    }

    int32_t physical_value::value(std::integral_constant<physical_type, physical_type::INT32>) const noexcept {
        assert(static_cast<physical_type>(*data_) == physical_type::INT32);
        return *reinterpret_cast<int32_t*>(data_ + 1);
    }

    int64_t physical_value::value(std::integral_constant<physical_type, physical_type::INT64>) const noexcept {
        assert(static_cast<physical_type>(*data_) == physical_type::INT64);
        return *reinterpret_cast<int64_t*>(data_ + 1);
    }

    float physical_value::value(std::integral_constant<physical_type, physical_type::FLOAT>) const noexcept {
        assert(static_cast<physical_type>(*data_) == physical_type::FLOAT);
        return *reinterpret_cast<float*>(data_ + 1);
    }

    double physical_value::value(std::integral_constant<physical_type, physical_type::DOUBLE>) const noexcept {
        assert(static_cast<physical_type>(*data_) == physical_type::DOUBLE);
        return *reinterpret_cast<double*>(data_ + 1);
    }

    std::string physical_value::value(std::integral_constant<physical_type, physical_type::STRING>) const noexcept {
        assert(static_cast<physical_type>(*data_) == physical_type::STRING);
        return std::string((char*) (data_ + 1 + sizeof(uint32_t)), *reinterpret_cast<uint32_t*>(data_ + 1));
    }
} // namespace components::types