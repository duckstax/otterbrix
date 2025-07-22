#pragma once

#include <algorithm>
#include <memory>
#include <optional>
#include <set>
#include <vector>

namespace components::catalog {
    template<typename Value>
    struct versioned_entry {
        versioned_entry(Value v)
            : value(std::move(v)) {}

        void add_ref() const { ++ref_count; }

        void release_ref() const {
            if (ref_count > 0) {
                --ref_count;
            }
        }

        bool is_alive() const { return ref_count > 0; }

        Value value;
        mutable uint64_t ref_count = 0;
    };

    template<typename Value>
    class versioned_value {
    public:
        versioned_value() = default;

        versioned_value(std::pmr::memory_resource* resource)
            : versions_(resource) {}

        std::size_t size() const { return versions_.size(); }

        std::optional<std::size_t> latest_version_id() const {
            return versions_.empty() ? std::optional<std::size_t>{} : get_last_element()->first;
        };

        versioned_entry<Value>& get_version(std::size_t idx) const { return versions_.at(idx); }

        void add_version(std::size_t version, Value v) const {
            versions_.emplace(version, versioned_entry<Value>(std::move(v)));
        }

        void remove_latest_version() {
            if (!versions_.empty()) {
                versions_.erase(get_last_element());
            }
        }

        std::size_t cleanup_dead_versions() {
            std::size_t cleared = 0;
            for (auto it = versions_.begin(); it != versions_.end();) {
                if (!it->second.is_alive()) {
                    it = versions_.erase(it);
                    ++cleared;
                } else {
                    ++it;
                }
            }
            return cleared;
        }

        bool has_alive_versions() const {
            for (auto it = versions_.begin(); it != versions_.end(); ++it) {
                if (it->second.is_alive()) {
                    return true;
                }
            }

            return false;
        }

        void swap(versioned_value& other) { std::swap(versions_, other.versions_); }

    private:
        auto get_last_element() const { return std::prev(versions_.end()); }

        mutable std::pmr::map<std::size_t, versioned_entry<Value>> versions_;
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
} // namespace components::catalog