#pragma once
#include "absl/container/btree_map.h"

namespace core::btree {

    template <typename Key, typename Value, typename Compare = std::less<Key>,
             typename Alloc = std::allocator<std::pair<const Key, Value>>>
    using btree_t = absl::btree_map<Key,Value,Compare,Alloc>;
}