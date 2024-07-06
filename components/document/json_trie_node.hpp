#pragma once

#include "container/json_array.hpp"
#include "container/json_object.hpp"
#include <components/document/base.hpp>
#include <components/document/impl/allocator_intrusive_ref_counter.hpp>
#include <components/document/impl/common_defs.hpp>
#include <components/document/impl/document.hpp>
#include <components/document/impl/element.hpp>
#include <components/document/impl/mr_utils.hpp>

namespace components::document::json {

    enum json_type
    {
        OBJECT,
        ARRAY,
        IMMUT,
        MUT,
        DELETER,
    };

    using immutable_part = impl::element<impl::immutable_document>;
    using mutable_part = impl::element<impl::mutable_document>;

    class json_trie_node : public allocator_intrusive_ref_counter<json_trie_node> {
    public:
        using allocator_type = std::pmr::memory_resource;

        ~json_trie_node() override;

        json_trie_node(json_trie_node&&) noexcept;

        json_trie_node(const json_trie_node&) = delete;

        json_trie_node& operator=(json_trie_node&&) noexcept;

        json_trie_node& operator=(const json_trie_node&) = delete;

        json_trie_node* make_deep_copy() const;

        json_type type() const noexcept;

        bool is_object() const noexcept;

        bool is_array() const noexcept;

        bool is_immut() const noexcept;

        bool is_mut() const noexcept;

        bool is_deleter() const noexcept;

        const immutable_part* get_immut() const;

        const mutable_part* get_mut() const;

        const json_array* get_array() const;

        const json_object* get_object() const;

        json_array* as_array();

        json_object* as_object();

        std::pmr::string to_json(std::pmr::string (*)(const immutable_part*, std::pmr::memory_resource*),
                                 std::pmr::string (*)(const mutable_part*, std::pmr::memory_resource*)) const;

        bool equals(const json_trie_node* other,
                    bool (*)(const immutable_part*, const immutable_part*),
                    bool (*)(const mutable_part*, const mutable_part*),
                    bool (*)(const immutable_part*, const mutable_part*)) const;

        static json_trie_node* merge(json_trie_node* node1, json_trie_node* node2, allocator_type* allocator);

        static json_trie_node* create(immutable_part value, allocator_type* allocator);

        static json_trie_node* create(mutable_part value, allocator_type* allocator);

        static json_trie_node* create_array(allocator_type* allocator);

        static json_trie_node* create_object(allocator_type* allocator);

        static json_trie_node* create_deleter(allocator_type* allocator);

    protected:
        allocator_type* get_allocator() override;

    private:
        allocator_type* allocator_;

        union value_type {
            json_object obj;
            json_array arr;
            immutable_part immut;
            mutable_part mut;

            value_type(json_object&& value)
                : obj(std::move(value)) {}

            value_type(json_array&& value)
                : arr(std::move(value)) {}

            value_type(immutable_part value)
                : immut(value) {}

            value_type(mutable_part value)
                : mut(value) {}

            value_type() {}
            ~value_type() {}
        } value_;

        json_type type_;

        template<typename T>
        json_trie_node(allocator_type* allocator, T&& value, json_type type) noexcept;

        json_trie_node(allocator_type* allocator, json_type type) noexcept;
    };

    template<typename T>
    json_trie_node::json_trie_node(allocator_type* allocator, T&& value, json_type type) noexcept
        : allocator_(allocator)
        , value_(std::forward<T>(value))
        , type_(type) {}

} // namespace  components::document::json