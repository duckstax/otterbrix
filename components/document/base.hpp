#pragma once

#include <absl/numeric/int128.h>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cmath>
#include <limits>
#include <memory_resource>

namespace components::document {

    using int128_t = absl::int128;

    template<typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
    static inline bool is_equals(T x, T y) {
        return std::fabs(x - y) < std::numeric_limits<T>::epsilon();
    }

    namespace impl {
        class element;
        class base_document;
    } // namespace impl

    namespace json {
        class json_trie_node;
        class json_array;
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