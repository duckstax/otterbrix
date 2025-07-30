#pragma once

#include <algorithm>
#include <memory>
#include <optional>
#include <utility>

#include <core/pmr.hpp>

namespace components::catalog {
    template<typename Key, typename Value>
    struct versioned_trie_node;

    struct trie_match_result {
        trie_match_result();

        trie_match_result(void const* n, std::ptrdiff_t s, bool m, bool l);

        void const* node;
        std::ptrdiff_t size;
        bool match;
        bool leaf;

        friend bool operator==(trie_match_result const& lhs, trie_match_result const& rhs) {
            return lhs.node == rhs.node && lhs.size == rhs.size && lhs.match == rhs.match && lhs.leaf == rhs.leaf;
        }

        friend bool operator!=(trie_match_result const& lhs, trie_match_result const& rhs) { return !(lhs == rhs); }
    };

    struct parent_index {
        parent_index();

        std::size_t value() const;

        template<typename Key, typename Value, typename Iter>
        void insert_at(core::pmr::unique_ptr<versioned_trie_node<Key, Value>> const& child,
                       std::ptrdiff_t offset,
                       Iter it,
                       Iter end) {
            child->parent_index_.value_ = static_cast<std::size_t>(offset);
            for (; it != end; ++it) {
                ++(*it)->parent_index_.value_;
            }
        }

        template<typename Key, typename Value>
        void insert_ptr(core::pmr::unique_ptr<versioned_trie_node<Key, Value>> const& child) {
            child->parent_index_.value_ = 0;
        }

        template<typename Iter>
        void erase(Iter it, Iter end) {
            for (; it != end; ++it) {
                --(*it)->parent_index_.value_;
            }
        }

    private:
        std::size_t value_;
    };

    template<typename Key, typename Value>
    struct trie_iterator_state_t {
        versioned_trie_node<Key, Value> const* parent_;
        std::size_t index_;
    };

    template<typename Key, typename Value>
    trie_iterator_state_t<Key, Value> parent_state(trie_iterator_state_t<Key, Value> state) {
        return {state.parent_->parent(), state.parent_->index_within_parent()};
    }

    template<typename Key, typename Value>
    Key reconstruct_key(trie_iterator_state_t<Key, Value> state) {
        Key retval;
        while (state.parent_->parent()) {
            retval.insert(retval.end(), state.parent_->key(state.index_));
            state = parent_state(state);
        }
        std::reverse(retval.begin(), retval.end());
        return retval;
    }

    template<typename Key, typename Value>
    versioned_trie_node<Key, Value> const* to_node(trie_iterator_state_t<Key, Value> state) {
        if (state.index_ < state.parent_->size())
            return state.parent_->child(state.index_);
        return nullptr;
    }
} // namespace components::catalog