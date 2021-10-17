#pragma once

#include "concurrent_arena.hpp"
#include "slice.hpp"
#include <atomic>
#include <memory>

namespace storage {

class concurrent_map_t {
public:
    static constexpr int max_capacity = 0x7FFF;
    static constexpr int max_string_capacity = 0x10000;

    using value_t = uint16_t;
    enum class hash_t : uint32_t {};

    struct result {
        slice_t key;
        value_t value;
    };

    concurrent_map_t(int capacity, int string_capacity = 0);
    concurrent_map_t(concurrent_map_t &&map);
    concurrent_map_t& operator=(concurrent_map_t &&map);

    static inline hash_t hash_code(slice_t key) PURE { return hash_t( key.hash() ); }

    int count() const PURE;
    int capacity() const PURE;
    int table_size() const PURE;
    int string_bytes_capacity() const PURE;
    int string_bytes_count() const PURE;

    result find(slice_t key) const noexcept PURE;
    result find(slice_t key, hash_t hash) const noexcept PURE;

    result insert(slice_t key, value_t value);
    result insert(slice_t key, value_t value, hash_t hash);

    bool remove(slice_t key);
    bool remove(slice_t key, hash_t hash);

    void dump() const;

private:
    struct entry_t {
        uint16_t key_offset;
        uint16_t value;

        uint32_t& as_int32();
        bool compare_and_swap(entry_t expected, entry_t swap_with);
    };

    int _size_mask;
    int _capacity;
    std::atomic<int> _count {0};
    concurrent_arena_t  _heap;
    entry_t *_entries;
    size_t _keys_offset;

    inline int wrap(int i) const              { return i & _size_mask; }
    inline int index_of_hash(hash_t h) const  { return wrap(int(h)); }
    const char* alloc_key(slice_t key);
    bool free_key(const char *alloced_key);

    inline uint16_t key_to_offset(const char *alloced_key) const PURE;
    inline const char* offset_to_key(uint16_t offset) const PURE;
};

}
