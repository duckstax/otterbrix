#include "trie_utils.hpp"

namespace components::catalog {
    trie_match_result::trie_match_result()
        : node(nullptr)
        , size(0)
        , match(false)
        , leaf(false) {}

    trie_match_result::trie_match_result(void const* n, std::ptrdiff_t s, bool m, bool l)
        : node(n)
        , size(s)
        , match(m)
        , leaf(l) {}

    parent_index::parent_index()
        : value_(std::numeric_limits<std::size_t>::max()) {}

    std::size_t parent_index::value() const { return value_; }
} // namespace components::catalog
