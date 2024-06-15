#pragma once

#include "container/json_array.hpp"
#include "container/json_object.hpp"
#include <components/new_document/base.hpp>
#include <components/new_document/impl/allocator_intrusive_ref_counter.hpp>
#include <components/new_document/impl/common_defs.h>
#include <components/new_document/impl/mr_utils.hpp>

namespace components::document::json {
    template<typename FirstType, typename SecondType>
    class json_trie_node : public allocator_intrusive_ref_counter<json_trie_node<FirstType, SecondType>> {
    public:
        enum json_type
        {
            OBJECT,
            ARRAY,
            FIRST,
            SECOND,
            DELETER,
        };
        using allocator_type = std::pmr::memory_resource;

        ~json_trie_node() override;

        json_trie_node(json_trie_node&&) noexcept;

        json_trie_node(const json_trie_node&) = delete;

        json_trie_node& operator=(json_trie_node&&) noexcept = delete;

        json_trie_node& operator=(const json_trie_node&) = delete;

        json_trie_node<FirstType, SecondType>* make_deep_copy() const;

        bool is_object() const noexcept;

        bool is_array() const noexcept;

        bool is_first() const noexcept;

        bool is_second() const noexcept;

        bool is_deleter() const noexcept;

        const FirstType* get_first() const;

        const SecondType* get_second() const;

        const json_array<FirstType, SecondType>* get_array() const;

        const json_object<FirstType, SecondType>* get_object() const;

        json_array<FirstType, SecondType>* as_array();

        json_object<FirstType, SecondType>* as_object();

        std::pmr::string to_json(std::pmr::string (*)(const FirstType*, std::pmr::memory_resource*),
                                 std::pmr::string (*)(const SecondType*, std::pmr::memory_resource*)) const;

        std::pmr::string to_binary(std::pmr::string (*)(const FirstType*, std::pmr::memory_resource*),
                                   std::pmr::string (*)(const SecondType*, std::pmr::memory_resource*)) const;

        bool equals(const json_trie_node<FirstType, SecondType>* other,
                    bool (*)(const FirstType*, const FirstType*),
                    bool (*)(const SecondType*, const SecondType*),
                    bool (*)(const FirstType*, const SecondType*)) const;

        static json_trie_node<FirstType, SecondType>* merge(json_trie_node<FirstType, SecondType>* node1,
                                                            json_trie_node<FirstType, SecondType>* node2,
                                                            allocator_type* allocator);

        static json_trie_node<FirstType, SecondType>* create(FirstType value, allocator_type* allocator);

        static json_trie_node<FirstType, SecondType>* create(SecondType value, allocator_type* allocator);

        static json_trie_node<FirstType, SecondType>* create_array(allocator_type* allocator);

        static json_trie_node<FirstType, SecondType>* create_object(allocator_type* allocator);

        static json_trie_node<FirstType, SecondType>* create_deleter(allocator_type* allocator);

    protected:
        allocator_type* get_allocator() override;

    private:
        allocator_type* allocator_;

        union value_type {
            json_object<FirstType, SecondType> obj;
            json_array<FirstType, SecondType> arr;
            FirstType first;
            SecondType second;

            value_type(json_object<FirstType, SecondType>&& value)
                : obj(std::move(value)){};

            value_type(json_array<FirstType, SecondType>&& value)
                : arr(std::move(value)){};

            value_type(FirstType value)
                : first(value){};

            value_type(SecondType value)
                : second(value){};

            value_type(){};
            ~value_type(){};
        } value_;

        json_type type_;

        template<typename T>
        json_trie_node(allocator_type* allocator, T&& value, json_type type) noexcept;

        json_trie_node(allocator_type* allocator, json_type type) noexcept;
    };

    template<typename FirstType, typename SecondType>
    template<typename T>
    json_trie_node<FirstType, SecondType>::json_trie_node(allocator_type* allocator, T&& value, json_type type) noexcept
        : allocator_(allocator)
        , value_(std::forward<T>(value))
        , type_(type) {}

    template<typename FirstType, typename SecondType>
    json_trie_node<FirstType, SecondType>::json_trie_node(allocator_type* allocator, json_type type) noexcept
        : allocator_(allocator)
        , value_()
        , type_(type) {}

    template<typename FirstType, typename SecondType>
    json_trie_node<FirstType, SecondType>::~json_trie_node() {
        switch (type_) {
            case OBJECT:
                value_.obj.~json_object<FirstType, SecondType>();
                break;
            case ARRAY:
                value_.arr.~json_array<FirstType, SecondType>();
                break;
            case FIRST:
                value_.first.~FirstType();
                break;
            case SECOND:
                value_.second.~SecondType();
                break;
        }
    }

    template<typename FirstType, typename SecondType>
    json_trie_node<FirstType, SecondType>::json_trie_node(json_trie_node&& other) noexcept
        : allocator_(other.allocator_)
        , value_(std::move(other.value_))
        , type_(other.type_) {
        other.allocator_ = nullptr;
    }

    template<typename FirstType, typename SecondType>
    json_trie_node<FirstType, SecondType>* json_trie_node<FirstType, SecondType>::make_deep_copy() const {
        switch (type_) {
            case OBJECT: {
                auto* ptr = value_.obj.make_deep_copy();
                size_t size = sizeof(*ptr);
                auto* copy = new (allocator_->allocate(sizeof(json_trie_node)))
                    json_trie_node(allocator_, std::move(*ptr), OBJECT);
                allocator_->deallocate(ptr, size);
                return copy;
            }
            case ARRAY: {
                auto* ptr = value_.arr.make_deep_copy();
                size_t size = sizeof(*ptr);
                auto* copy = new (allocator_->allocate(sizeof(json_trie_node)))
                    json_trie_node(allocator_, std::move(*ptr), ARRAY);
                allocator_->deallocate(ptr, size);
                return copy;
            }
            case FIRST:
                return create(value_.first, allocator_);
            case SECOND:
                return create(value_.second, allocator_);
            case DELETER:
                return create_deleter(allocator_);
        }
    }

    template<typename FirstType, typename SecondType>
    bool json_trie_node<FirstType, SecondType>::is_first() const noexcept {
        return type_ == FIRST;
    }

    template<typename FirstType, typename SecondType>
    bool json_trie_node<FirstType, SecondType>::is_second() const noexcept {
        return type_ == SECOND;
    }

    template<typename FirstType, typename SecondType>
    bool json_trie_node<FirstType, SecondType>::is_object() const noexcept {
        return type_ == OBJECT;
    }

    template<typename FirstType, typename SecondType>
    bool json_trie_node<FirstType, SecondType>::is_array() const noexcept {
        return type_ == ARRAY;
    }

    template<typename FirstType, typename SecondType>
    bool json_trie_node<FirstType, SecondType>::is_deleter() const noexcept {
        return type_ == DELETER;
    }

    template<typename FirstType, typename SecondType>
    const FirstType* json_trie_node<FirstType, SecondType>::get_first() const {
        if (_usually_false(!is_first())) {
            return nullptr;
        }
        return &value_.first;
    }

    template<typename FirstType, typename SecondType>
    const SecondType* json_trie_node<FirstType, SecondType>::get_second() const {
        if (_usually_false(!is_second())) {
            return nullptr;
        }
        return &value_.second;
    }

    template<typename FirstType, typename SecondType>
    const json_array<FirstType, SecondType>* json_trie_node<FirstType, SecondType>::get_array() const {
        if (_usually_false(!is_array())) {
            return nullptr;
        }
        return &value_.arr;
    }

    template<typename FirstType, typename SecondType>
    const json_object<FirstType, SecondType>* json_trie_node<FirstType, SecondType>::get_object() const {
        if (_usually_false(!is_object())) {
            return nullptr;
        }
        return &value_.obj;
    }

    template<typename FirstType, typename SecondType>
    json_array<FirstType, SecondType>* json_trie_node<FirstType, SecondType>::as_array() {
        return const_cast<json_array<FirstType, SecondType>*>(get_array());
    }

    template<typename FirstType, typename SecondType>
    json_object<FirstType, SecondType>* json_trie_node<FirstType, SecondType>::as_object() {
        return const_cast<json_object<FirstType, SecondType>*>(get_object());
    }

    template<typename FirstType, typename SecondType>
    std::pmr::string json_trie_node<FirstType, SecondType>::to_json(
        std::pmr::string (*to_json_first)(const FirstType*, std::pmr::memory_resource*),
        std::pmr::string (*to_json_second)(const SecondType*, std::pmr::memory_resource*)) const {
        switch (type_) {
            case OBJECT:
                return value_.obj.to_json(to_json_first, to_json_second);
            case ARRAY:
                return value_.arr.to_json(to_json_first, to_json_second);
            case FIRST:
                return to_json_first(&value_.first, allocator_);
            case SECOND:
                return to_json_second(&value_.second, allocator_);
            case DELETER:
                return {"DELETER", allocator_};
        }
    }

    template<typename FirstType, typename SecondType>
    std::pmr::string json_trie_node<FirstType, SecondType>::to_binary(
        std::pmr::string (*to_binary_first)(const FirstType*, std::pmr::memory_resource*),
        std::pmr::string (*to_binary_second)(const SecondType*, std::pmr::memory_resource*)) const {
        switch (type_) {
            case OBJECT:
                return value_.obj.to_binary(to_binary_first, to_binary_second);
            case ARRAY:
                return value_.arr.to_binary(to_binary_first, to_binary_second);
            case FIRST:
                return to_binary_first(&value_.first, allocator_);
            case SECOND:
                return to_binary_second(&value_.second, allocator_);
            case DELETER:
                return {"DELETER", allocator_};
        }
    }

    template<typename FirstType, typename SecondType>
    bool json_trie_node<FirstType, SecondType>::equals(const json_trie_node<FirstType, SecondType>* other,
                                                       bool (*first_equals_first)(const FirstType*, const FirstType*),
                                                       bool (*second_equals_second)(const SecondType*,
                                                                                    const SecondType*),
                                                       bool (*first_equals_second)(const FirstType*,
                                                                                   const SecondType*)) const {
        if (type_ != other->type_) {
            if (type_ == FIRST && other->type_ == SECOND) {
                return first_equals_second(&value_.first, &other->value_.second);
            }
            if (type_ == SECOND && other->type_ == FIRST) {
                return first_equals_second(&other->value_.first, &value_.second);
            }
            return false;
        }
        switch (type_) {
            case OBJECT:
                return value_.obj.equals(other->value_.obj,
                                         first_equals_first,
                                         second_equals_second,
                                         first_equals_second);
            case ARRAY:
                return value_.arr.equals(other->value_.arr,
                                         first_equals_first,
                                         second_equals_second,
                                         first_equals_second);
            case FIRST:
                return first_equals_first(&value_.first, &other->value_.first);
            case SECOND:
                return second_equals_second(&value_.second, &other->value_.second);
            case DELETER:
                return true;
        }
    }

    template<typename FirstType, typename SecondType>
    json_trie_node<FirstType, SecondType>*
    json_trie_node<FirstType, SecondType>::merge(json_trie_node<FirstType, SecondType>* node1,
                                                 json_trie_node<FirstType, SecondType>* node2,
                                                 allocator_type* allocator) {
        if (!node2->is_object()) {
            return node2;
        }
        auto res = create_object(allocator);
        if (node1->is_object()) {
            auto* ptr = json_object<FirstType, SecondType>::merge(node1->value_.obj, node2->value_.obj, allocator);
            size_t size = sizeof(*ptr);
            res->value_.obj = std::move(*ptr);
            allocator->deallocate(ptr, size);
        } else {
            auto* ptr = node1->value_.obj.make_copy_except_deleter(allocator);
            size_t size = sizeof(*ptr);
            res->value_.obj = std::move(*ptr);
            allocator->deallocate(ptr, size);
        }
        return res;
    }

    template<typename FirstType, typename SecondType>
    json_trie_node<FirstType, SecondType>*
    json_trie_node<FirstType, SecondType>::create(FirstType value, json_trie_node::allocator_type* allocator) {
        return new (allocator->allocate(sizeof(json_trie_node<FirstType, SecondType>)))
            json_trie_node(allocator, value, FIRST);
    }

    template<typename FirstType, typename SecondType>
    json_trie_node<FirstType, SecondType>*
    json_trie_node<FirstType, SecondType>::create(SecondType value, json_trie_node::allocator_type* allocator) {
        return new (allocator->allocate(sizeof(json_trie_node<FirstType, SecondType>)))
            json_trie_node(allocator, value, SECOND);
    }

    template<typename FirstType, typename SecondType>
    json_trie_node<FirstType, SecondType>*
    json_trie_node<FirstType, SecondType>::create_array(json_trie_node::allocator_type* allocator) {
        return new (allocator->allocate(sizeof(json_trie_node<FirstType, SecondType>)))
            json_trie_node(allocator, std::move((json_array<FirstType, SecondType>(allocator))), ARRAY);
    }

    template<typename FirstType, typename SecondType>
    json_trie_node<FirstType, SecondType>*
    json_trie_node<FirstType, SecondType>::create_object(json_trie_node::allocator_type* allocator) {
        return new (allocator->allocate(sizeof(json_trie_node<FirstType, SecondType>)))
            json_trie_node(allocator, std::move((json_object<FirstType, SecondType>(allocator))), OBJECT);
    }

    template<typename FirstType, typename SecondType>
    json_trie_node<FirstType, SecondType>*
    json_trie_node<FirstType, SecondType>::create_deleter(json_trie_node::allocator_type* allocator) {
        return new (allocator->allocate(sizeof(json_trie_node<FirstType, SecondType>)))
            json_trie_node(allocator, DELETER);
    }

    template<typename FirstType, typename SecondType>
    typename json_trie_node<FirstType, SecondType>::allocator_type*
    json_trie_node<FirstType, SecondType>::get_allocator() {
        return allocator_;
    }
} // namespace  components::document::json