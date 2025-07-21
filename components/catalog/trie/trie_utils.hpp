#pragma once

#include <algorithm>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace components::catalog {
    template<typename Key, typename Value>
    struct trie_node_t;

    template<typename Value>
    struct versioned_entry {
        versioned_entry(uint64_t version, Value v)
            : version(version)
            , value(std::move(v)) {}

        void add_ref() const { ++ref_count; }

        void release_ref() const {
            if (ref_count > 0) {
                --ref_count;
            }
        }

        bool is_alive() const { return ref_count > 0; }

        uint64_t version;
        Value value;
        mutable uint32_t ref_count = 0;
    };

    template<typename Value>
    class versioned_value {
    public:
        versioned_value() { versions_.reserve(100); }

        const versioned_entry<Value>* get_latest_version() const {
            if (versions_.empty())
                return nullptr;
            return std::addressof(versions_.back());
        }

        const std::vector<versioned_entry<Value>>& versions() const { return versions_; }

        void add_version(uint64_t version, Value v) const { versions_.emplace_back(version, std::move(v)); }

        void remove_latest_version() {
            if (!versions_.empty()) {
                versions_.erase(versions_.end() - 1);
            }
        }

        bool has_live_versions() const {
            return std::any_of(versions_.begin(), versions_.end(), [](const versioned_entry<Value>& v) {
                return v.is_alive();
            });
        }

        void swap(versioned_value& other) { std::swap(versions_, other.versions_); }

    private:
        mutable std::vector<versioned_entry<Value>> versions_;
    };

    template<typename Key, typename Value>
    struct trie_element {
        trie_element() {}

        trie_element(Key k, Value v)
            : key(k)
            , value(v) {}

        template<typename KeyT, typename ValueT>
        trie_element(trie_element<KeyT, ValueT> const& rhs)
            : key(rhs.key)
            , value(rhs.value) {}

        template<typename KeyT, typename ValueT>
        trie_element(trie_element<KeyT, ValueT>&& rhs)
            : key(std::move(rhs.key))
            , value(std::move(rhs.value)) {}

        template<typename KeyT, typename ValueT>
        trie_element& operator=(trie_element<KeyT, ValueT> const& rhs) {
            key = rhs.key;
            value = rhs.value;
            return *this;
        }

        template<typename KeyT, typename ValueT>
        trie_element& operator=(trie_element<KeyT, ValueT>&& rhs) {
            key = std::move(rhs.key);
            value = std::move(rhs.value);
            return *this;
        }

        Key key;
        Value value;

        friend bool operator==(trie_element const& lhs, trie_element const& rhs) {
            return lhs.key == rhs.key && lhs.value == rhs.value;
        }
        friend bool operator!=(trie_element const& lhs, trie_element const& rhs) { return !(lhs == rhs); }
    };

    /** The result type for trie operations that produce a matching
        subsequence. */
    struct trie_match_result {
        trie_match_result()
            : node(nullptr)
            , size(0)
            , match(false)
            , leaf(false) {}

        trie_match_result(void const* n, std::ptrdiff_t s, bool m, bool l)
            : node(n)
            , size(s)
            , match(m)
            , leaf(l) {}

        void const* node;
        std::ptrdiff_t size;

        /** True iff this result represents a match.  Stated another way,
            `match` is true iff `node` represents an element in the trie
            (whether or not `node` has children). */
        bool match;
        bool leaf;

        friend bool operator==(trie_match_result const& lhs, trie_match_result const& rhs) {
            return lhs.node == rhs.node && lhs.size == rhs.size && lhs.match == rhs.match && lhs.leaf == rhs.leaf;
        }

        friend bool operator!=(trie_match_result const& lhs, trie_match_result const& rhs) { return !(lhs == rhs); }
    };

    struct parent_index {
        parent_index()
            : value_(-1) {}

        std::size_t value() const { return value_; }

        template<typename Key, typename Value, typename Iter>
        void
        insert_at(std::unique_ptr<trie_node_t<Key, Value>> const& child, std::ptrdiff_t offset, Iter it, Iter end) {
            child->parent_index_.value_ = offset;
            for (; it != end; ++it) {
                ++(*it)->parent_index_.value_;
            }
        }

        template<typename Key, typename Value>
        void insert_ptr(std::unique_ptr<trie_node_t<Key, Value>> const& child) {
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
        trie_node_t<Key, Value> const* parent_;
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
    trie_node_t<Key, Value> const* to_node(trie_iterator_state_t<Key, Value> state) {
        if (state.index_ < state.parent_->size())
            return state.parent_->child(state.index_);
        return nullptr;
    }
} // namespace components::catalog