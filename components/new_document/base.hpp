#pragma once

#include <absl/numeric/int128.h>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cmath>
#include <limits>
#include <memory_resource>

namespace components::document {

    using int128_t = absl::int128;

    template<typename T>
    static inline bool is_equals(T x, T y) {
        static_assert(std::is_floating_point<T>());
        return std::fabs(x - y) < std::numeric_limits<T>::epsilon();
    }

    namespace json {
        template<typename FirstType, typename SecondType>
        class json_trie_node;
        template<typename FirstType, typename SecondType>
        class json_array;
        template<typename FirstType, typename SecondType>
        class json_object;
    } // namespace json

    enum binary_type : uint8_t
    {
        tape,
        object_start,
        object_end,
        array_start,
        array_end
    };

} // namespace components::document