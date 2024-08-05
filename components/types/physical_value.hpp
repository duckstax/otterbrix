#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>

#include "types.hpp"

namespace components::types {

    // note: possibly add a memory resource
    class physical_value {
    public:
        // currently supported values
        explicit physical_value(); // nullptr_t
        explicit physical_value(bool);
        explicit physical_value(uint8_t);
        explicit physical_value(uint16_t);
        explicit physical_value(uint32_t);
        explicit physical_value(uint64_t);
        explicit physical_value(int8_t);
        explicit physical_value(int16_t);
        explicit physical_value(int32_t);
        explicit physical_value(int64_t);
        explicit physical_value(float);
        explicit physical_value(double);
        explicit physical_value(const std::string& value);

        physical_value(const physical_value& other);
        physical_value(physical_value&& other) noexcept;
        physical_value& operator=(const physical_value& other);
        physical_value& operator=(physical_value&& other) noexcept;
        ~physical_value();

        // if types convertable to each other, compared by value, otherwise returns physical type order
        bool operator<(const physical_value& other) const noexcept;
        bool operator>(const physical_value& other) const noexcept;
        bool operator==(const physical_value& other) const noexcept;
        bool operator!=(const physical_value& other) const noexcept;
        bool operator<=(const physical_value& other) const noexcept;
        bool operator>=(const physical_value& other) const noexcept;

        template<physical_type Type>
        auto value() const noexcept {
            return value(std::integral_constant<physical_type, Type>{});
        }

        physical_type type() const noexcept;

    private:
        nullptr_t value(std::integral_constant<physical_type, physical_type::NA>) const noexcept;
        bool value(std::integral_constant<physical_type, physical_type::BOOL_FALSE>) const noexcept;
        bool value(std::integral_constant<physical_type, physical_type::BOOL_TRUE>) const noexcept;
        uint8_t value(std::integral_constant<physical_type, physical_type::UINT8>) const noexcept;
        uint16_t value(std::integral_constant<physical_type, physical_type::UINT16>) const noexcept;
        uint32_t value(std::integral_constant<physical_type, physical_type::UINT32>) const noexcept;
        uint64_t value(std::integral_constant<physical_type, physical_type::UINT64>) const noexcept;
        int8_t value(std::integral_constant<physical_type, physical_type::INT8>) const noexcept;
        int16_t value(std::integral_constant<physical_type, physical_type::INT16>) const noexcept;
        int32_t value(std::integral_constant<physical_type, physical_type::INT32>) const noexcept;
        int64_t value(std::integral_constant<physical_type, physical_type::INT64>) const noexcept;
        float value(std::integral_constant<physical_type, physical_type::FLOAT>) const noexcept;
        double value(std::integral_constant<physical_type, physical_type::DOUBLE>) const noexcept;
        std::string value(std::integral_constant<physical_type, physical_type::STRING>) const noexcept;

        uint8_t* data_ = nullptr;
    };

} // namespace components::types