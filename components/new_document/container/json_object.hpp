#pragma once

#include <absl/container/flat_hash_map.h>
#include <components/new_document/base.hpp>

namespace components::new_document::json {
    struct string_view_hash {
        using is_transparent = void;

        size_t operator()(const std::pmr::string& s) const { return std::hash<std::pmr::string>{}(s); }

        size_t operator()(std::string_view sv) const { return std::hash<std::string_view>{}(sv); }
    };

    struct string_view_eq {
        using is_transparent = void;

        bool operator()(const std::pmr::string& lhs, const std::pmr::string& rhs) const noexcept { return lhs == rhs; }

        bool operator()(const std::pmr::string& lhs, std::string_view rhs) const noexcept { return lhs == rhs; }

        bool operator()(std::string_view lhs, const std::pmr::string& rhs) const noexcept { return lhs == rhs; }
    };

    class json_object {
        using flat_map_t = absl::flat_hash_map<
            std::pmr::string,
            boost::intrusive_ptr<json_trie_node>,
            string_view_hash,
            string_view_eq,
            std::pmr::polymorphic_allocator<std::pair<const std::pmr::string, boost::intrusive_ptr<json_trie_node>>>>;
        using iterator = flat_map_t::iterator;
        using const_iterator = flat_map_t::const_iterator;

    public:
        using allocator_type = std::pmr::memory_resource;

        explicit json_object(allocator_type* allocator) noexcept;

        ~json_object() = default;

        json_object(json_object&&) = default;

        json_object(const json_object&) = delete;

        json_object& operator=(json_object&&) = default;

        json_object& operator=(const json_object&) = delete;

        const json_trie_node* get(std::string_view key) const;

        iterator begin();
        iterator end();
        const_iterator begin() const;
        const_iterator end() const;
        const_iterator cbegin() const;
        const_iterator cend() const;

        void set(std::string_view key, json_trie_node* value);

        void set(std::string_view key, boost::intrusive_ptr<json_trie_node>&& value);

        boost::intrusive_ptr<json_trie_node> remove(std::string_view key);

        size_t size() const noexcept;

        json_object* make_deep_copy() const;

        std::pmr::string to_json(std::pmr::string (*)(const immutable_part*, std::pmr::memory_resource*),
                                 std::pmr::string (*)(const mutable_part*, std::pmr::memory_resource*)) const;

        bool equals(const json_object& other,
                    bool (*)(const immutable_part*, const immutable_part*),
                    bool (*)(const mutable_part*, const mutable_part*),
                    bool (*)(const immutable_part*, const mutable_part*)) const;

        json_object* make_copy_except_deleter(allocator_type* allocator) const;

        static json_object* merge(json_object& object1, json_object& object2, allocator_type* allocator);

    private:
        flat_map_t map_;
    };

} // namespace components::new_document::json