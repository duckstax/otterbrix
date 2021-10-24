#include "concurrent_map.hpp"
#include <algorithm>
#include <cmath>

#if defined(_WIN32) || defined(_WIN64)
#include <Windows.h>
#endif

using namespace std;

namespace storage {

static constexpr size_t min_initial_size     = 16;
static constexpr float max_load              = 0.6f;
static constexpr uint16_t empty_key_offset   = 0;
static constexpr uint16_t deleted_key_offset = 1;
static constexpr uint16_t min_key_offset     = 2;

static inline bool atomic_compare_and_swap(uint32_t *value, uint32_t old_value, uint32_t new_value) {
#if defined(_WIN32) || defined(_WIN64)
    return InterlockedCompareExchange(value, new_value, old_value) == old_value;
#else
    return __sync_bool_compare_and_swap(value, old_value, new_value);
#endif
}

PURE static inline bool equals_keys(const char *key_ptr, slice_t key) {
    return memcmp(key_ptr, key.buf, key.size) == 0 && key_ptr[key.size] == 0;
}


uint32_t &concurrent_map_t::entry_t::as_int32()
{
    return *(uint32_t*)this;
}

bool concurrent_map_t::entry_t::compare_and_swap(entry_t expected, entry_t swap_with) {
    static_assert(sizeof(entry_t) == 4);
    return atomic_compare_and_swap(&as_int32(), expected.as_int32(), swap_with.as_int32());
}


concurrent_map_t::concurrent_map_t(int capacity, int string_capacity) {
    precondition(capacity <= max_capacity);
    int size;
    for (size = min_initial_size; size * max_load < capacity; size *= 2);
    _capacity = int(floor(size * max_load));
    _size_mask = size - 1;

    if (string_capacity == 0)
        string_capacity = 17 * _capacity;
    string_capacity = min(string_capacity, int(max_string_capacity));
    size_t table_size = size * sizeof(entry_t);

    _heap = concurrent_arena_t(table_size + string_capacity);
    _entries = concurrent_arena_allocator_t<entry_t, true>(_heap).allocate(size);
    _keys_offset = table_size - min_key_offset;

    postcondition(static_cast<int>(_heap.available()) == string_capacity);
}

concurrent_map_t::concurrent_map_t(concurrent_map_t &&map) {
    *this = move(map);
}

concurrent_map_t& concurrent_map_t::operator=(concurrent_map_t &&map) {
    _size_mask = map._size_mask;
    _capacity = map._capacity;
    _count = map._count.load();
    _entries = map._entries;
    _heap = move(map._heap);
    return *this;
}

int concurrent_map_t::count() const {
    return _count;
}

int concurrent_map_t::capacity() const {
    return _capacity;
}

int concurrent_map_t::table_size() const {
    return _size_mask + 1;
}

int concurrent_map_t::string_bytes_capacity() const {
    return int(_heap.capacity() - (_keys_offset + min_key_offset));
}

int concurrent_map_t::string_bytes_count() const {
    return int(_heap.allocated() - (_keys_offset + min_key_offset));
}

inline uint16_t concurrent_map_t::key_to_offset(const char *alloced_key) const {
    ptrdiff_t result = _heap.to_offset(alloced_key) - _keys_offset;
    assert(result >= min_key_offset && result <= UINT16_MAX);
    return uint16_t(result);
}

inline const char* concurrent_map_t::offset_to_key(uint16_t offset) const {
    assert(offset >= min_key_offset);
    return (const char*)_heap.to_pointer(_keys_offset + offset);
}

concurrent_map_t::result concurrent_map_t::find(slice_t key) const noexcept {
    return find(key, hash_code(key));
}

concurrent_map_t::result concurrent_map_t::find(slice_t key, hash_t hash) const noexcept {
    assert_precondition(key);
    for (int i = index_of_hash(hash); true; i = wrap(i + 1)) {
        entry_t current = _entries[i];
        switch (current.key_offset) {
        case empty_key_offset:
            return {};
        case deleted_key_offset:
            break;
        default:
            if (auto key_ptr = offset_to_key(current.key_offset); equals_keys(key_ptr, key))
                return {slice_t(key_ptr, key.size), current.value};
            break;
        }
    }
}

concurrent_map_t::result concurrent_map_t::insert(slice_t key, value_t value) {
    return insert(key, value, hash_code(key));
}

concurrent_map_t::result concurrent_map_t::insert(slice_t key, value_t value, hash_t hash) {
    assert_precondition(key);
    const char *alloced_key = nullptr;
    int i = index_of_hash(hash);
    while (true) {
retry:
        entry_t current = _entries[i];
        switch (current.key_offset) {
        case empty_key_offset:
        case deleted_key_offset: {
            if (!alloced_key) {
                if (_count >= _capacity)
                    return {};
                alloced_key = alloc_key(key);
                if (!alloced_key)
                    return {};
            }
            entry_t new_entry = {key_to_offset(alloced_key), value};
            if (_usually_false(!_entries[i].compare_and_swap(current, new_entry))) {
                goto retry;
            }
            ++_count;
            assert(_count <= _capacity);
            return {slice_t(alloced_key, key.size), value};
        }
        default:
            if (auto key_ptr = offset_to_key(current.key_offset); equals_keys(key_ptr, key)) {
                free_key(alloced_key);
                return {slice_t(key_ptr, key.size), current.value};
            }
            break;
        }
        i = wrap(i + 1);
    }
}

bool concurrent_map_t::remove(slice_t key) {
    return remove(key, hash_code(key));
}

bool concurrent_map_t::remove(slice_t key, hash_t hash) {
    assert_precondition(key);
    int i = index_of_hash(hash);
    while (true) {
retry:
        entry_t current = _entries[i];
        switch (current.key_offset) {
        case empty_key_offset:
            return false;
        case deleted_key_offset:
            break;
        default:
            if (auto key_ptr = offset_to_key(current.key_offset); equals_keys(key_ptr, key)) {
                entry_t tombstone = {deleted_key_offset, current.value};
                if (_usually_false(!_entries[i].compare_and_swap(current, tombstone))) {
                    goto retry;
                }
                --_count;
                (void)free_key(key_ptr);
                return true;
            }
            break;
        }
        i = wrap(i + 1);
    }
}

const char* concurrent_map_t::alloc_key(slice_t key) {
    auto result = (char*)_heap.alloc(key.size + 1);
    if (result) {
        key.copy_to(result);
        result[key.size] = 0;
    }
    return result;
}

bool concurrent_map_t::free_key(const char *alloced_key) {
    return alloced_key == nullptr || _heap.free((void*)alloced_key, strlen(alloced_key) + 1);
}

void concurrent_map_t::dump() const {
    int size = table_size();
    int real_count = 0, tombstones = 0, total_distance = 0, max_distance = 0;
    for (int i = 0; i < size; i++) {
        auto e = _entries[i];
        switch (e.key_offset) {
        case empty_key_offset:
            printf("%6d\n", i);
            break;
        case deleted_key_offset:
            ++tombstones;
            printf("%6d xxx\n", i);
            break;
        default: {
            ++real_count;
            auto key_ptr = offset_to_key(e.key_offset);
            hash_t hash = hash_code(slice_t(key_ptr));
            int best_index = index_of_hash(hash);
            printf("%6d: %-10s = %08x [%5d]", i, key_ptr, hash, best_index);
            if (i != best_index) {
                if (best_index > i)
                    best_index -= size;
                auto distance = i - best_index;
                printf(" +%d", distance);
                total_distance += distance;
                max_distance = max(max_distance, distance);
            }
            printf("\n");
        }
        }
    }
    printf("Occupancy = %d / %d (%.0f%%), with %d tombstones\n",
           real_count, size, real_count/double(size)*100.0, tombstones);
    printf("Average probes = %.1f, max probes = %d\n",
           1.0 + (total_distance / (double)real_count), max_distance);
}

}
