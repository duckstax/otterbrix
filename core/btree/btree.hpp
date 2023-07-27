#pragma once

#include <core/pmr.hpp>

#include "absl/container/btree_map.h"

namespace core::pmr::btree {
    template<
        typename Key,
        typename Value,
        typename Compare = std::less<Key>>
<<<<<<< HEAD
    using btree_t = absl::btree_map<Key, Value, Compare, core::pmr::polymorphic_allocator<std::pair<const Key, Value>>>;
=======
    using btree_t = absl::btree_map<Key, Value, Compare, std::pmr::polymorphic_allocator<std::pair<const Key, Value>>>;

    template<
        typename Key,
        typename Value,
        typename Compare = std::less<Key>>
    using multi_btree_t = absl::btree_multimap<Key, Value, Compare, std::pmr::polymorphic_allocator<std::pair<const Key, Value>>>;
>>>>>>> develop
}