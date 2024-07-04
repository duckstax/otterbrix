#pragma once

#include <absl/numeric/int128.h>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cmath>
#include <limits>
#include <memory_resource>

namespace components::document {

    using int128_t = __int128_t;

    template<typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
    static inline bool is_equals(T x, T y) {
        return std::fabs(x - y) < std::numeric_limits<T>::epsilon();
    }

    namespace impl {
        template<typename K>
        class element;
        class immutable_document;
        class mutable_document;
    } // namespace impl

    namespace json {
        class json_trie_node;
        class json_array;
        class json_object;

        using immutable_part = impl::element<impl::immutable_document>;
        using mutable_part = impl::element<impl::mutable_document>;
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