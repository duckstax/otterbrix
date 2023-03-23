#pragma once

#include "cstdint"

#include "serializer/byte_container.hpp"

namespace components::document::impl {
        class dict_t;
        class array_t;
}

namespace components::document {

    struct type_traits final {

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
        using array = impl::array_t;
        using dict = impl::dict_t;
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

    };

} // namespace components::document