#pragma once

#include <components/document/support/platform_compat.hpp>
#include <components/document/core/slice.hpp>
#include <algorithm>
#include <utility>

namespace document {

class string_table_t
{
public:

    enum class hash_t : uint32_t {
        empty = 0
    };

    using key_t   = slice_t;
    using value_t = uint32_t;
    using entry_t = std::pair<key_t, value_t>;
    using insert_result_t = std::pair<entry_t*, bool>;

    string_table_t(size_t capacity = 0);
    string_table_t(const string_table_t &s);
    string_table_t& operator=(const string_table_t &s);
    ~string_table_t();

    static inline hash_t hash_code(key_t key) PURE {
        return hash_t(std::max(key.hash(), 1u));
    }

    size_t count() const PURE { return _count; }
    size_t size() const PURE  { return _size; }

    void clear() noexcept;

    const entry_t* find(key_t key) const noexcept PURE;
    const entry_t* find(key_t key, hash_t) const noexcept PURE;

    insert_result_t insert(key_t key, value_t value);
    insert_result_t insert(key_t key, value_t value, hash_t);

    void insert_only(key_t key, value_t value);
    void insert_only(key_t key, value_t value, hash_t);

    void dump() const noexcept;

protected:
    string_table_t(size_t capacity, size_t initial_size, hash_t *initial_hashes, entry_t *initial_entries);
    inline size_t wrap(size_t i) const         { return i & _size_mask; }
    inline size_t index_of_hash(hash_t h) const     { return wrap(size_t(h)); }
    void _insert_only(hash_t hash, entry_t entry) noexcept;
    void alloc_table(size_t size);
    void grow();
    void init_table(size_t size, hash_t *hashes, entry_t *entries);

    size_t _size;
    size_t _size_mask;
    size_t _count {0};
    size_t _capacity;
    size_t _max_distance;
    hash_t*  _hashes;
    entry_t* _entries;
    bool _allocated {false};
};


template <size_t INITIAL_SIZE = 32>
class preallocated_string_table_t : public string_table_t
{
public:
    preallocated_string_table_t(size_t capacity = 0)
        : string_table_t(capacity, INITIAL_SIZE, _initial_hashes, _initial_entries)
    {}

private:
    hash_t  _initial_hashes[INITIAL_SIZE];
    entry_t _initial_entries[INITIAL_SIZE];
};

}
