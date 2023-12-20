#include "tools.hpp"

#include <core/assert/assert.hpp>

#include <dataframe/detail/bitmask.hpp>
#include <dataframe/detail/bits.hpp>

namespace components::dataframe::test {

    size_type
    valid_count(std::pmr::memory_resource* resource, bitmask_type const* bitmask, size_type start, size_type stop) {
        if (bitmask == nullptr) {
            assertion_exception_msg(start >= 0 or start <= stop, "invalid range.");
            auto const total_num_bits = (stop - start);
            return total_num_bits;
        }

        return detail::count_set_bits(resource, bitmask, start, stop);
    }

    void sequence(core::buffer& buffer) {
        auto* ptr = static_cast<char*>(buffer.data());
        auto size = buffer.size();
        for (int i = 0; i < size; ++i) {
            ptr[i] = i;
        }
    }

    bool equal(core::buffer& buffer1, core::buffer& buffer2) {
        bool result = false;
        if (buffer1.data() == nullptr && buffer2.data() == nullptr && buffer1.size() != buffer2.size()) {
            return result;
        }

        result = std::equal(static_cast<char*>(buffer1.data()),
                            static_cast<char*>(buffer1.data()) + buffer1.size(),
                            static_cast<char*>(buffer2.data()),
                            static_cast<char*>(buffer2.data()) + buffer1.size());

        return result;
    }

    bool equal(const void* lhs, const void* rhs, std::size_t size) {
        auto* lhs_tmp = const_cast<char*>(reinterpret_cast<const char*>(lhs));
        auto* rhs_tmp = const_cast<char*>(reinterpret_cast<const char*>(rhs));

        return std::equal(lhs_tmp, lhs_tmp + size, rhs_tmp, rhs_tmp + size);
    }
} // namespace components::dataframe::test