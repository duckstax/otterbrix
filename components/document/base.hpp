#pragma once

#include <absl/numeric/int128.h>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cmath>
#include <limits>
#include <memory_resource>

namespace components::new_document {

    using int128_t = absl::int128;

    template<typename T>
    static inline bool is_equals(T x, T y) {
        static_assert(std::is_floating_point<T>());
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

} // namespace components::new_document