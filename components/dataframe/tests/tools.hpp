#pragma once

#include <memory_resource>

#include <core/buffer.hpp>

#include <dataframe/detail/bits.hpp>
#include <dataframe/types.hpp>

namespace components::dataframe::test {

    size_type
    valid_count(std::pmr::memory_resource* resource, bitmask_type const* bitmask, size_type start, size_type stop);

    template<typename ValidityIterator>
    std::vector<bitmask_type> make_null_mask_vector(ValidityIterator begin, ValidityIterator end) {
        auto const size = std::distance(begin, end);
        auto const num_words = bitmask_allocation_size_bytes(size) / sizeof(bitmask_type);

        auto null_mask = std::vector<bitmask_type>(num_words, 0);
        for (auto i = 0; i < size; ++i)
            if (*(begin + i))
                detail::set_bit(null_mask.data(), i, true);

        return null_mask;
    }

    template<typename ValidityIterator>
    core::buffer make_null_mask(std::pmr::memory_resource* resource, ValidityIterator begin, ValidityIterator end) {
        auto null_mask = make_null_mask_vector(begin, end);
        return core::buffer{
            resource,
            null_mask.data(),
            null_mask.size() * sizeof(decltype(null_mask.front())),
        };
    }

    void sequence(core::buffer& buffer);

    template<class T1, class T2>
    void sequence(T1* ptr1, T2* ptr2) {
        auto size = ptr2 - ptr1;
        for (int i = 0; i < size; ++i) {
            ptr1[i] = i;
        }
    }

    bool equal(core::buffer& buffer1, core::buffer& buffer2);
    bool equal(const void* lhs, const void* rhs, std::size_t size);

    template<class T>
    void copy(T* ptr_start, T* ptr_finish, T* target) {
        std::copy(ptr_start, ptr_finish, target);
    }
} // namespace components::dataframe::test