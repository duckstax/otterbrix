#pragma once

#include <memory_resource>

#include "absl/container/node_hash_map.h"

namespace core::pmr::hash_map {
    template<class Key,
             class Value,
             class Hash = absl::container_internal::hash_default_hash<Key>,
             class Eq = absl::container_internal::hash_default_eq<Key>>
    using hash_map = absl::node_hash_map<Key, Value, Hash, Eq, std::pmr::polymorphic_allocator<std::pair<const Key, Value>>>;
}