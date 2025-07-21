#pragma once

#include "trie_boost_node.hpp"
#include "trie_utils.hpp"
#include <cassert>

namespace components::catalog {
    template<typename T>
    struct proxy_arrow_result {
        constexpr explicit proxy_arrow_result(T value)
            : value_(std::move(value)) {}

        constexpr T const* operator->() const noexcept { return &value_; }
        constexpr T* operator->() noexcept { return &value_; }

    private:
        T value_;
    };

    template<typename Key, typename Value>
    struct const_trie_map_iterator {
    private:
        using iterator_category = std::forward_iterator_tag;
        using value_type = trie_element<Key, Value>;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;
        using reference = trie_element<Key, Value const&>;
        using ptr_type = proxy_arrow_result<reference>;

    public:
        const_trie_map_iterator()
            : state_{nullptr, 0}
            , version_(nullptr) {}

        const_trie_map_iterator(trie_match_result match_result) {
            assert(match_result.node);
            assert(match_result.match);
            auto node = static_cast<trie_node_t<Key, Value> const*>(match_result.node);
            state_.parent_ = node->parent();
            state_.index_ = node->index_within_parent();
            version_ = state_.parent_->child_value(state_.index_).get_latest_version();
            acquire_current_version();
        }

        const_trie_map_iterator(const const_trie_map_iterator& other)
            : state_(other.state_)
            , version_(nullptr) {
            if (other.version_) {
                version_ = other.version_;
                acquire_current_version();
            }
        }

        const_trie_map_iterator& operator=(const const_trie_map_iterator& other) {
            if (this != &other) {
                release_current_version();
                state_ = other.state_;
                if (other.version_) {
                    version_ = other.version_;
                    acquire_current_version();
                }
            }
            return *this;
        }

        ~const_trie_map_iterator() { release_current_version(); }

        reference operator*() const { return reference{reconstruct_key(state_), version_->value}; }
        ptr_type operator->() const {
            reference&& deref_result = **this;
            return ptr_type(reference{std::move(deref_result.key), deref_result.value});
        }

        const_trie_map_iterator& operator++() {
            auto node = to_node(state_);
            if (node && !node->empty()) {
                state_.parent_ = node;
                state_.index_ = 0;
            } else {
                // Try the next sibling node.
                ++state_.index_;
                auto const first_state = state_;
                while (state_.parent_->parent() && state_.parent_->parent()->parent() &&
                       state_.parent_->size() <= state_.index_) {
                    state_ = parent_state(state_);
                    ++state_.index_;
                }

                // If we went all the way up, incrementing indices, and they
                // were all at size() for each node, the first increment above
                // must have taken us to the end; use that.
                if ((!state_.parent_->parent() || !state_.parent_->parent()->parent()) &&
                    state_.parent_->size() <= state_.index_) {
                    state_ = first_state;
                    release_current_version();
                    return *this;
                }
            }

            node = state_.parent_->child(state_.index_);
            release_current_version();
            while (!node->has_versions()) {
                auto i = 0u;
                node = node->child(i);
                state_ = state_t{node->parent(), i};
                version_ = node->latest_version();
                acquire_current_version();
            }

            return *this;
        }

        const_trie_map_iterator operator++(int) {
            auto copy = *this;
            ++(*this);
            return copy;
        }

        friend bool operator==(const_trie_map_iterator lhs, const_trie_map_iterator rhs) {
            return lhs.state_.parent_ == rhs.state_.parent_ && lhs.state_.index_ == rhs.state_.index_ &&
                   lhs.version_ == rhs.version_;
        }

        friend bool operator!=(const_trie_map_iterator lhs, const_trie_map_iterator rhs) { return !(lhs == rhs); }

    private:
        void release_current_version() {
            if (version_) {
                version_->release_ref();
                version_ = nullptr;
            }
        }

        void acquire_current_version() {
            if (version_) {
                version_->add_ref();
            }
        }

        using state_t = trie_iterator_state_t<Key, Value>;
        state_t state_;
        const versioned_entry<Value>* version_;

        explicit const_trie_map_iterator(state_t state)
            : state_(state)
            , version_(nullptr) {
            if (state_.parent_->size() > state_.index_) {
                version_ = state_.parent_->child_value(state_.index_).get_latest_version();
                acquire_current_version();
            }
        }

        template<typename KeyT, typename ValueT, typename Compare>
        friend struct trie_map;
        template<typename KeyT, typename ValueT>
        friend struct trie_map_iterator;
    };

    template<typename Key, typename Value>
    struct trie_map_iterator {
    private:
        const_trie_map_iterator<Key, Value> it_;
        using iterator_category = std::forward_iterator_tag;
        using value_type = trie_element<Key, Value>;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;
        using reference = trie_element<Key, Value&>;
        using ptr_type = proxy_arrow_result<reference>;

    public:
        trie_map_iterator() {}

        trie_map_iterator(trie_match_result match_result)
            : trie_map_iterator(const_trie_map_iterator<Key, Value>(match_result)) {}

        reference operator*() const {
            return reference{reconstruct_key(it_.state_), const_cast<Value&>(it_.version_->value)};
        };

        ptr_type operator->() const {
            reference&& deref_result = **this;
            return ptr_type(reference{std::move(deref_result.key), deref_result.value});
        }

        trie_map_iterator& operator++() {
            ++it_;
            return *this;
        }

        trie_map_iterator& operator++(int) {
            it_++;
            return *this;
        }

        friend bool operator==(trie_map_iterator lhs, trie_map_iterator rhs) { return lhs.it_ == rhs.it_; }

        friend bool operator!=(trie_map_iterator lhs, trie_map_iterator rhs) { return !(lhs == rhs); }

    private:
        explicit trie_map_iterator(trie_iterator_state_t<Key, Value> state)
            : it_(state) {}
        explicit trie_map_iterator(const_trie_map_iterator<Key, Value> it)
            : it_(it) {}

        template<typename KeyT, typename ValueT, typename Compare>
        friend struct trie_map;
    };
} // namespace components::catalog
