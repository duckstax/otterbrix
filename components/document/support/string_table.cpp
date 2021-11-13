#include "string_table.hpp"
#include "platform_compat.hpp"
#include <algorithm>
#include <stdlib.h>
#include <vector>
#include "better_assert.hpp"

namespace document {

static constexpr size_t min_initial_size = 16;
static const float max_load = 0.9f;


string_table_t::string_table_t(size_t capacity)
    : string_table_t(capacity, min_initial_size, nullptr, nullptr)
{}

string_table_t::string_table_t(size_t capacity, size_t initial_size, hash_t *initial_hashes, entry_t *initial_entries)
{
    size_t size;
    for (size = initial_size; size * max_load < capacity; size *= 2);
    if (initial_hashes && size <= initial_size)
        init_table(size, initial_hashes, initial_entries);
    else
        alloc_table(size);
}

string_table_t::string_table_t(const string_table_t &s) {
    *this = s;
}

string_table_t& string_table_t::operator=(const string_table_t &s) {
    if (_allocated) {
        free(_hashes);
        _hashes = nullptr;
    }
    _entries = nullptr;
    _allocated = false;
    alloc_table(s._size);
    _count = s._count;
    _max_distance = s._max_distance;
    memcpy(_hashes, s._hashes, _count * sizeof(hash_t));
    memcpy(_entries, s._entries, _count * sizeof(entry_t));
    return *this;
}

string_table_t::~string_table_t() {
    if (_allocated)
        free(_hashes);
}

void string_table_t::clear() noexcept {
    ::memset(_hashes, 0, _size * sizeof(hash_t));
    _count = 0;
    _max_distance = 0;
}

const string_table_t::entry_t* string_table_t::find(key_t key) const noexcept {
    return find(key, hash_code(key));
}

const string_table_t::entry_t* string_table_t::find(key_t key, hash_t hash) const noexcept {
    assert_precondition(key.buf != nullptr);
    assert_precondition(hash != hash_t::empty);
    size_t end = wrap(size_t(hash) + _max_distance + 1);
    for (size_t i = index_of_hash(hash); i != end; i = wrap(i + 1)) {
        if (_hashes[i] == hash_t::empty)
            break;
        else if (_hashes[i] == hash && _entries[i].first == key)
            return &_entries[i];
    }
    return nullptr;
}

string_table_t::insert_result_t string_table_t::insert(key_t key, value_t value) {
    return insert(key, value, hash_code(key));
}

string_table_t::insert_result_t string_table_t::insert(key_t key, value_t value, hash_t hash) {
    assert_precondition(key);
    assert_precondition(hash != hash_t::empty);

    if (_usually_false(_count > _capacity))
        grow();

    size_t distance = 0;
    auto max_distance = _max_distance;
    hash_t cur_hash = hash;
    entry_t cur_entry = {key, value};
    entry_t *result = nullptr;
    size_t i;
    for (i = index_of_hash(hash); _hashes[i] != hash_t::empty; i = wrap(i + 1)) {
        assert(distance < _count);
        if (_hashes[i] == hash && _entries[i].first == key) {
            if (!result)
                return {&_entries[i], false};
            else
                break;
        }
        size_t its_distance = wrap(i - index_of_hash(_hashes[i]) + _size);
        if (its_distance < distance) {
            std::swap(cur_hash,  _hashes[i]);
            std::swap(cur_entry, _entries[i]);
            max_distance = std::max(distance, max_distance);
            distance = its_distance;
            if (!result)
                result = &_entries[i];
        }
        ++distance;
    }

    _hashes[i]  = cur_hash;
    _entries[i] = std::move(cur_entry);
    _max_distance = std::max(distance, max_distance);
    ++_count;

    if (!result)
        result = &_entries[i];
    return {result, true};
}

void string_table_t::insert_only(key_t key, value_t value) {
    insert_only(key, value, hash_code(key));
}

void string_table_t::insert_only(key_t key, value_t value, hash_t hash) {
    assert_precondition(!find(key, hash));
    if (_usually_false(++_count > _capacity))
        grow();
    _insert_only(hash, {key, value});
}

void string_table_t::_insert_only(hash_t hash, entry_t entry) noexcept {
    assert_precondition(entry.first);
    assert_precondition(hash != hash_t::empty);
    size_t distance = 0;
    auto max_distance = _max_distance;
    size_t i;
    for (i = index_of_hash(hash); _hashes[i] != hash_t::empty; i = wrap(i + 1)) {
        assert(distance < _count);
        size_t its_distance = wrap(i - index_of_hash(_hashes[i]) + _size);
        if (its_distance < distance) {
            std::swap(hash,  _hashes[i]);
            std::swap(entry, _entries[i]);
            max_distance = std::max(distance, max_distance);
            distance = its_distance;
        }
        ++distance;
    }

    _hashes[i]  = hash;
    _entries[i] = std::move(entry);
    _max_distance = std::max(distance, max_distance);
}

void string_table_t::init_table(size_t size, hash_t *hashes, entry_t *entries) {
    _size = size;
    _size_mask = size - 1;
    _max_distance = 0;
    _capacity = (size_t)(size * max_load);
    _hashes = hashes;
    _entries = entries;
    memset(_hashes, 0, size * sizeof(hash_t));
}

void string_table_t::alloc_table(size_t size) {
    size_t hashes_size  = size * sizeof(hash_t), entries_size = size * sizeof(entry_t);
    void *memory = ::malloc(hashes_size + entries_size);
    if (!memory)
        throw std::bad_alloc();
    init_table(size, (hash_t*)memory, (entry_t*)offsetby(memory, hashes_size));
    _allocated = true;
}

void string_table_t::grow() {
    auto old_size = _size;
    auto old_hashes = _hashes;
    auto old_entries = _entries;
    auto was_allocated = _allocated;

    alloc_table(2 * old_size);
    for (size_t i = 0; i < old_size; ++i) {
        if (old_hashes[i] != hash_t::empty)
            _insert_only(old_hashes[i], old_entries[i]);
    }
    if (was_allocated)
        free(old_hashes);
}

void string_table_t::dump() const noexcept {
    size_t total_distance = 0;
    std::vector<size_t> distance_counts(_max_distance+1);
    for (size_t i = 0; i < _size; ++i) {
        printf("%4zd: ", i);
        if (_hashes[i] != hash_t::empty) {
            key_t key = _entries[i].first;
            size_t index = index_of_hash(hash_code(key));
            size_t distance = (i - (int)index + _size) & _size_mask;
            total_distance += distance;
            ++distance_counts[distance];
            printf("(%2zd) '%.*s'\n", distance, SLICE(key));
        } else {
            printf("--\n");
        }
    }
    printf(">> Capacity %zd, using %zu (%.0f%%)\n", _size, _count,  _count/(double)_size*100.0);
    printf(">> Average key distance = %.2f, max = %zd\n", total_distance/(double)count(), _max_distance);
    for (size_t i = 0; i <= _max_distance; ++i)
        printf("\t%2zd: %zd\n", i, distance_counts[i]);
}

}
