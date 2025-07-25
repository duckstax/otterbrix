#pragma once

#include "trie_utils.hpp"
#include "versioned_trie_node.hpp"
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
    struct const_versioned_trie_iterator {
    private:
        using iterator_category = std::forward_iterator_tag;
        using value_type = trie_element<Key, Value>;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;
        using reference = trie_element<Key, Value const&>;
        using ptr_type = proxy_arrow_result<reference>;

    public:
        const_versioned_trie_iterator()
            : state_{nullptr, 0} {}

        const_versioned_trie_iterator(trie_match_result match_result) {
            assert(match_result.node);
            assert(match_result.match);
            auto node = static_cast<versioned_trie_node<Key, Value> const*>(match_result.node);
            state_.parent_ = node->parent();
            state_.index_ = node->index_within_parent();
            set_latest_version(state_.parent_->child_value(state_.index_));
            acquire_current_version();
        }

        const_versioned_trie_iterator(const const_versioned_trie_iterator& other)
            : state_(other.state_)
            , version_index_(other.version_index_) {
            if (other.version_) {
                version_ = other.version_;
                acquire_current_version();
            }
        }

        const_versioned_trie_iterator& operator=(const const_versioned_trie_iterator& other) {
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

        ~const_versioned_trie_iterator() { release_current_version(); }

        reference operator*() const {
            return reference{reconstruct_key(state_), version_->get_version(version_index_).value};
        }
        ptr_type operator->() const {
            reference&& deref_result = **this;
            return ptr_type(reference{std::move(deref_result.key), deref_result.value});
        }

        versioned_entry<Value>& ref_counted() { return version_->get_version(version_index_); };

        const_versioned_trie_iterator& operator++() {
            auto node = const_cast<versioned_trie_node<Key, Value>*>(to_node(state_));
            if (node && !node->empty()) {
                state_.parent_ = node;
                state_.index_ = 0;
            } else {
                // try the next sibling node.
                ++state_.index_;
                auto const first_state = state_;
                while (state_.parent_->parent() && state_.parent_->parent()->parent() &&
                       state_.parent_->size() <= state_.index_) {
                    state_ = parent_state(state_);
                    ++state_.index_;
                }

                // if we went all the way up, incrementing indices, and they were all at size() for each node,
                // the first increment above must have taken us to the end; use that.
                if ((!state_.parent_->parent() || !state_.parent_->parent()->parent()) &&
                    state_.parent_->size() <= state_.index_) {
                    state_ = first_state;
                    release_current_version();
                    return *this;
                }
            }

            node = const_cast<versioned_trie_node<Key, Value>*>(state_.parent_->child(state_.index_));
            release_current_version();
            if (node->has_versions()) {
                set_latest_version(node->value());
            }

            while (!node->has_versions()) {
                auto i = 0u;
                node = node->child(i);
                state_ = state_t{node->parent(), i};
                set_latest_version(node->value());
            }
            acquire_current_version();

            return *this;
        }

        const_versioned_trie_iterator operator++(int) {
            auto copy = *this;
            ++(*this);
            return copy;
        }

        friend bool operator==(const_versioned_trie_iterator lhs, const_versioned_trie_iterator rhs) {
            return lhs.state_.parent_ == rhs.state_.parent_ && lhs.state_.index_ == rhs.state_.index_ &&
                   lhs.version_ == rhs.version_;
        }

        friend bool operator!=(const_versioned_trie_iterator lhs, const_versioned_trie_iterator rhs) {
            return !(lhs == rhs);
        }

    private:
        using state_t = trie_iterator_state_t<Key, Value>;
        explicit const_versioned_trie_iterator(state_t state)
            : state_(state) {
            if (state_.parent_->size() > state_.index_) {
                set_latest_version(state_.parent_->child_value(state_.index_));
                acquire_current_version();
            }
        }

        void release_current_version() {
            if (version_ && version_index_ <= version_->latest_version_id()) {
                version_->get_version(version_index_).release_ref();
                version_index_ = 0;
                version_ = nullptr;
            }
        }

        void acquire_current_version() {
            if (version_) {
                version_->get_version(version_index_).add_ref();
            }
        }

        void set_latest_version(versioned_value<Value>& v) {
            version_ = std::addressof(v);
            version_index_ = version_->latest_version_id().value_or(0);
        }

        state_t state_;
        const versioned_value<Value>* version_ = nullptr;
        std::size_t version_index_ = 0;

        template<typename KeyT, typename ValueT, typename Compare>
        friend struct versioned_trie;
        template<typename KeyT, typename ValueT>
        friend struct versioned_trie_iterator;
    };

    template<typename Key, typename Value>
    struct versioned_trie_iterator {
    private:
        using iterator_category = std::forward_iterator_tag;
        using value_type = trie_element<Key, Value>;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;
        using reference = trie_element<Key, Value&>;
        using ptr_type = proxy_arrow_result<reference>;
        using state_t = trie_iterator_state_t<Key, Value>;

    public:
        versioned_trie_iterator() {}

        versioned_trie_iterator(trie_match_result match_result)
            : versioned_trie_iterator(const_versioned_trie_iterator<Key, Value>(match_result)) {}

        reference operator*() const {
            return reference{reconstruct_key(it_.state_),
                             const_cast<Value&>(it_.version_->get_version(it_.version_index_).value)};
        };

        ptr_type operator->() const {
            reference&& deref_result = **this;
            return ptr_type(reference{std::move(deref_result.key), deref_result.value});
        }

        versioned_entry<Value>& ref_counted() { return it_.ref_counted(); };

        versioned_trie_iterator& operator++() {
            ++it_;
            return *this;
        }

        versioned_trie_iterator& operator++(int) {
            it_++;
            return *this;
        }

        friend bool operator==(versioned_trie_iterator lhs, versioned_trie_iterator rhs) { return lhs.it_ == rhs.it_; }

        friend bool operator!=(versioned_trie_iterator lhs, versioned_trie_iterator rhs) { return !(lhs == rhs); }

    private:
        explicit versioned_trie_iterator(state_t state)
            : it_(state) {}
        explicit versioned_trie_iterator(const_versioned_trie_iterator<Key, Value> it)
            : it_(it) {}

        const_versioned_trie_iterator<Key, Value> it_;

        template<typename KeyT, typename ValueT, typename Compare>
        friend struct versioned_trie;
    };
} // namespace components::catalog
