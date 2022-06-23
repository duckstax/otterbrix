#pragma once

#include <cstdint>
#include <cstdlib>

#ifndef NDEBUG
#include <atomic>
#endif

namespace document::impl::internal {

    enum {
        size_narrow = 2,
        size_wide   = 4
    };

    enum tags : uint8_t {
        tag_short,
        tag_int,
        tag_float,
        tag_special,
        tag_string,
        tag_binary,
        tag_array,
        tag_dict,
        tag_pointer
    };

    enum {
        special_value_null       = 0x00,
        special_value_undefined  = 0x0C,
        special_value_false      = 0x04,
        special_value_true       = 0x08,
    };

    static const size_t min_shared_string_size =  2;
    static const size_t max_shared_string_size = 15;

    static const uint32_t long_array_count = 0x07FF;

    class pointer_t;
    class heap_value_t;
    class heap_collection_t;
    class heap_array_t;
    class heap_dict_t;

#ifdef NDEBUG
    constexpr bool is_disable_necessary_shared_keys_check = false;
#else
    extern bool is_disable_necessary_shared_keys_check;
    extern std::atomic<unsigned> total_comparisons;
#endif

#ifndef _MSC_VER
    #define EVEN_ALIGNED __attribute__((aligned(2)))
#else
    #define EVEN_ALIGNED
#endif


}
