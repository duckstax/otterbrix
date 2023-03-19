#pragma once

#include "cstdint"

#include "serializer/byte_container.hpp"

namespace components::document {

    struct type_traits final {
        using array_t = ::document::impl::array_t*;
        using binary_t = std::vector<std::uint8_t>;
        using boolean_t = bool;
        using number_float_t = double;
        using number_integer_t = std::int64_t;
        using number_unsigned_t = std::uint64_t;
        using string_t = std::string;

        enum value_t : std::uint8_t
        {
            null,             ///< null value
            object,           ///< object (unordered set of name/value pairs)
            array,            ///< array (ordered collection of values)
            string,           ///< string value
            boolean,          ///< boolean value
            number_integer,   ///< number value (signed integer)
            number_unsigned,  ///< number value (unsigned integer)
            number_float,     ///< number value (floating-point)
            binary,           ///< binary array (ordered collection of bytes)
            discarded         ///< discarded by the parser callback function
        };

        enum input_format_t { json, cbor, msgpack, ubjson, bson, bjdata };

        union json_value
        {
            value_t valueType;
            /// array (stored with pointer to save storage)
            array_t array;
            /// string (stored with pointer to save storage)
            string_t* string;
            /// binary (stored with pointer to save storage)
            binary_t* binary;
            /// boolean
            boolean_t boolean;
            /// number (integer)
            number_integer_t number_integer;
            /// number (unsigned integer)
            number_unsigned_t number_unsigned;
            /// number (floating-point)
            number_float_t number_float;
            

            /// default constructor (for null values)
            json_value() = default;
            /// constructor for booleans
            json_value(boolean_t v) noexcept : boolean(v) {}
            /// constructor for numbers (integer)
            json_value(number_integer_t v) noexcept : number_integer(v) {}
            /// constructor for numbers (unsigned)
            json_value(number_unsigned_t v) noexcept : number_unsigned(v) {}
            /// constructor for numbers (floating-point)
            json_value(number_float_t v) noexcept : number_float(v) {}
            /// constructor for empty values of a given type

            auto type() const {
                return valueType;
            }
        };
    };

    struct type_traits_new final {
        using boolean = int8_t;
        using tinyint = int8_t;
        using smallint = int16_t;
        using integer = int32_t;
        using bigint = int64_t;
        using utinyint = uint8_t;
        using usmallint = uint16_t;
        using uinteger = uint32_t;
        using ubigint = uint64_t;
        using float32 = float;
        using float64 = double;
        using pointer = uintptr_t;
        using hash = uint64_t;
        using bytes = byte_container_t;
        using array = ::document::impl::array_t;
        using dict = ::document::impl::dict_t;
    };

    struct type_traits_old final {

        using boolean = int8_t;
        using tinyint = int8_t;
        using smallint = int16_t;
        using integer = int32_t;
        using bigint = int64_t;
        using utinyint = uint8_t;
        using usmallint = uint16_t;
        using uinteger = uint32_t;
        using ubigint = uint64_t;
        using float32 = float;
        using float64 = double;
        using pointer = uintptr_t;
        using hash = uint64_t;
        using bytes = byte_container_t;

    };

} // namespace components::document