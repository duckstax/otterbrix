#pragma once
#include <cstdint>
#include <string>

namespace components::types {

    enum class physical_type : uint8_t
    {
        BOOL_FALSE = 0,
        BOOL_TRUE = 1,
        UINT8 = 2,
        INT8 = 3,
        UINT16 = 4,
        INT16 = 5,
        UINT32 = 6,
        INT32 = 7,
        UINT64 = 8,
        INT64 = 9,
        UINT128 = 10,
        INT128 = 11,
        // HALF_FLOAT = 12,
        FLOAT = 13,
        DOUBLE = 14,
        STRING = 15,
        // BINARY = 16,                // Variable-length bytes (no guarantee of UTF8-ness)
        // FIXED_SIZE_BINARY = 17,     // Fixed-size binary. Each value occupies the same number of bytes
        // DATE32 = 18,                // int32_t days since the UNIX epoch
        // DATE64 = 19,                // int64_t milliseconds since the UNIX epoch
        // TIMESTAMP = 20,             // Exact timestamp encoded with int64 since UNIX epoch, ms
        // TIME32 = 21,                // Time as signed 32-bit integer, representing either seconds or mks since midnight
        // TIME64 = 22,                // Time as signed 64-bit integer, representing either mks or ns since midnight
        // INTERVAL = 23,              // YEAR_MONTH or DAY_TIME interval in SQL style
        // DECIMAL = 24,               // Precision- and scale-based decimal type. Storage type depends on the parameters.
        // LIST = 25,                  // A list of some logical data type
        // STRUCT = 26,                // Struct of logical types
        // UNION = 27,                 // Unions of logical types

        // Dictionary-encoded type, also called "categorical" or "factor"
        // in other programming languages. Holds the dictionary value
        // type but not the dictionary itself, which is part of the
        // ArrayData struct
        // DICTIONARY = 28,

        // EXTENSION = 29,             // Custom data type, implemented by user
        // ARRAY = 30,                 // Array with fixed length of some logical type (a fixed-size list)
        // DURATION = 31,              // Measure of elapsed time in either seconds, ms, mks or ns.
        // LARGE_STRING = 32,          // Like STRING, but with 64-bit offsets
        // LARGE_BINARY = 33,          // Like BINARY, but with 64-bit offsets
        // LARGE_LIST = 34,            // Like LIST, but with 64-bit offsets

        NA = 127,      // NULL value
        UNKNOWN = 128, // Unknown physical type of user defined types
        INVALID = 255
    };

    enum class logical_type : uint8_t
    {
        NA = 0,   // NULL type, used for constant NULL
        ANY = 1,  // ANY type, used for functions that accept any type as parameter
        USER = 2, // A User Defined Type (e.g., ENUMs before the binder)
        BOOLEAN = 10,
        TINYINT = 11,
        SMALLINT = 12,
        INTEGER = 13,
        BIGINT = 14,
        HUGEINT = 15,
        DATE = 16,
        TIME = 17,
        TIMESTAMP_SEC = 18,
        TIMESTAMP_MS = 19,
        TIMESTAMP_MKS = 20,
        TIMESTAMP_NS = 21,
        DECIMAL = 22,
        FLOAT = 23,
        DOUBLE = 24,
        CHAR = 25,
        BLOB = 26,
        INTERVAL = 27,
        UTINYINT = 28,
        USMALLINT = 29,
        UINTEGER = 30,
        UBIGINT = 31,
        UHUGEINT = 32,
        TIMESTAMP_TZ = 33,
        TIME_TZ = 34,
        BIT = 35,
        STRING_LITERAL = 36,  // String literals, used for constant strings
        INTEGER_LITERAL = 37, // Integer literals, used for constant integers

        POINTER = 51,
        VALIDITY = 53,
        UUID = 54,

        STRUCT = 100,
        LIST = 101,
        MAP = 102,
        TABLE = 103,
        ENUM = 104,
        AGGREGATE_STATE = 105,
        LAMBDA = 106,
        UNION = 107,
        ARRAY = 108,

        UNKNOWN = 127, // Unknown type, used for parameter expressions
        INVALID = 255
    };

    std::string type_name(logical_type type);
    std::string type_name(physical_type type);

    constexpr logical_type to_logical(physical_type type) {
        switch (type) {
            case physical_type::BOOL_FALSE:
            case physical_type::BOOL_TRUE:
                return logical_type::BOOLEAN;
            case physical_type::UINT8:
                return logical_type::UTINYINT;
            case physical_type::INT8:
                return logical_type::TINYINT;
            case physical_type::UINT16:
                return logical_type::USMALLINT;
            case physical_type::INT16:
                return logical_type::SMALLINT;
            case physical_type::UINT32:
                return logical_type::UINTEGER;
            case physical_type::INT32:
                return logical_type::INTEGER;
            case physical_type::UINT64:
                return logical_type::UBIGINT;
            case physical_type::INT64:
                return logical_type::BIGINT;
            case physical_type::UINT128:
                return logical_type::UHUGEINT;
            case physical_type::INT128:
                return logical_type::HUGEINT;
            case physical_type::FLOAT:
                return logical_type::FLOAT;
            case physical_type::DOUBLE:
                return logical_type::DOUBLE;
            case physical_type::STRING:
                return logical_type::STRING_LITERAL;
            case physical_type::NA:
                return logical_type::NA;
            case physical_type::UNKNOWN:
                return logical_type::UNKNOWN;
            default:
                return logical_type::UNKNOWN;
        }
    }

} // namespace components::types