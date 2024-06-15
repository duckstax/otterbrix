#pragma once

#include <components/new_document/base.hpp>

namespace components::document::json {
    template<typename FirstType, typename SecondType>
    class json_array {
    public:
        using allocator_type = std::pmr::memory_resource;

        explicit json_array(allocator_type* allocator) noexcept;

        ~json_array() = default;

        json_array(json_array&&) noexcept = default;

        json_array(const json_array&) = delete;

        json_array& operator=(json_array&&) noexcept = default;

        json_array& operator=(const json_array&) = delete;

        const json_trie_node<FirstType, SecondType>* get(uint32_t index) const;

        void set(uint32_t index, json_trie_node<FirstType, SecondType>* value);

        void set(uint32_t index, boost::intrusive_ptr<json_trie_node<FirstType, SecondType>>&& value);

        boost::intrusive_ptr<json_trie_node<FirstType, SecondType>> remove(uint32_t index);

        uint32_t size() const noexcept;

        json_array<FirstType, SecondType>* make_deep_copy() const;

        std::pmr::string to_json(std::pmr::string (*)(const FirstType*, std::pmr::memory_resource*),
                                 std::pmr::string (*)(const SecondType*, std::pmr::memory_resource*)) const;

        std::pmr::string to_binary(std::pmr::string (*)(const FirstType*, std::pmr::memory_resource*),
                                   std::pmr::string (*)(const SecondType*, std::pmr::memory_resource*)) const;

        bool equals(const json_array<FirstType, SecondType>& other,
                    bool (*)(const FirstType*, const FirstType*),
                    bool (*)(const SecondType*, const SecondType*),
                    bool (*)(const FirstType*, const SecondType*)) const;

    private:
        std::pmr::vector<boost::intrusive_ptr<json_trie_node<FirstType, SecondType>>> items_;
    };

    template<typename FirstType, typename SecondType>
    json_array<FirstType, SecondType>::json_array(json_array::allocator_type* allocator) noexcept
        : items_(allocator) {}

    template<typename FirstType, typename SecondType>
    const json_trie_node<FirstType, SecondType>* json_array<FirstType, SecondType>::get(uint32_t index) const {
        if (index >= size()) {
            return nullptr;
        }
        return items_[index].get();
    }

    template<typename FirstType, typename SecondType>
    void json_array<FirstType, SecondType>::set(uint32_t index, json_trie_node<FirstType, SecondType>* value) {
        if (index >= size()) {
            items_.emplace_back(value);
        } else {
            items_[index] = value;
        }
    }

    template<typename FirstType, typename SecondType>
    void json_array<FirstType, SecondType>::set(uint32_t index,
                                                boost::intrusive_ptr<json_trie_node<FirstType, SecondType>>&& value) {
        if (index >= size()) {
            items_.emplace_back(std::move(value));
        } else {
            items_[index] = std::move(value);
        }
    }

    template<typename FirstType, typename SecondType>
    boost::intrusive_ptr<json_trie_node<FirstType, SecondType>>
    json_array<FirstType, SecondType>::remove(uint32_t index) {
        if (index >= size()) {
            return nullptr;
        }
        auto copy = items_[index];
        items_.erase(items_.begin() + index);
        return copy;
    }

    template<typename FirstType, typename SecondType>
    uint32_t json_array<FirstType, SecondType>::size() const noexcept {
        return items_.size();
    }

    template<typename FirstType, typename SecondType>
    json_array<FirstType, SecondType>* json_array<FirstType, SecondType>::make_deep_copy() const {
        auto copy = new (items_.get_allocator().resource()->allocate(sizeof(json_array<FirstType, SecondType>)))
            json_array(items_.get_allocator().resource());
        for (uint32_t i = 0; i < items_.size(); ++i) {
            copy->items_[i] = items_[i]->make_deep_copy();
        }
        return copy;
    }

    template<typename FirstType, typename SecondType>
    std::pmr::string json_array<FirstType, SecondType>::to_json(
        std::pmr::string (*to_json_first)(const FirstType*, std::pmr::memory_resource*),
        std::pmr::string (*to_json_second)(const SecondType*, std::pmr::memory_resource*)) const {
        std::pmr::string res(items_.get_allocator().resource());
        res.append("[");
        for (auto& it : items_) {
            if (res.size() > 1) {
                res.append(",");
            }
            res.append(it->to_json(to_json_first, to_json_second));
        }
        return res.append("]");
        ;
    }

    template<typename FirstType, typename SecondType>
    std::pmr::string json_array<FirstType, SecondType>::to_binary(
        std::pmr::string (*to_binary_first)(const FirstType*, std::pmr::memory_resource*),
        std::pmr::string (*to_binary_second)(const SecondType*, std::pmr::memory_resource*)) const {
        std::pmr::string res(items_.get_allocator().resource());
        res.push_back(binary_type::array_start);
        for (auto& it : items_) {
            res.append(it->to_binary(to_binary_first, to_binary_second));
            res.push_back('\0');
        }
        res.push_back(binary_type::array_end);
        return res;
    }

    template<typename FirstType, typename SecondType>
    bool json_array<FirstType, SecondType>::equals(const json_array<FirstType, SecondType>& other,
                                                   bool (*first_equals_first)(const FirstType*, const FirstType*),
                                                   bool (*second_equals_second)(const SecondType*, const SecondType*),
                                                   bool (*first_equals_second)(const FirstType*,
                                                                               const SecondType*)) const {
        if (size() != other.size()) {
            return false;
        }
        for (uint32_t i = 0; i < size(); ++i) {
            if (!items_[i]->equals(other.items_[i].get(),
                                   first_equals_first,
                                   second_equals_second,
                                   first_equals_second)) {
                return false;
            }
        }
        return true;
    }
} // namespace components::document::json