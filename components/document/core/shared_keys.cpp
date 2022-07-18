#include "shared_keys.hpp"
#include <components/document/support/exception.hpp>
#include <components/document/core/value.hpp>
#include <components/document/core/array.hpp>

namespace document::impl {

key_t::key_t(slice_t key)
    : _string(key)
{
    assert_precondition(key);
}

key_t::key_t(int key)
    : _int(static_cast<int16_t>(key))
{
    assert_precondition(key >= 0 && key <= INT16_MAX);
}

key_t::key_t(const value_t *v) noexcept {
    if (v->is_int())
        _int = static_cast<int16_t>(v->as_int());
    else
        _string = v->as_string();
}

bool key_t::shared() const {
    return !_string;
}

int key_t::as_int() const {
    assert_precondition(shared());
    return _int;
}

slice_t key_t::as_string() const {
    return _string;
}

bool key_t::operator== (const key_t &k) const noexcept {
    return shared() ? (_int == k._int) : (_string == k._string);
}

bool key_t::operator< (const key_t &k) const noexcept {
    if (shared())
        return !k.shared() || (_int < k._int);
    else
        return !k.shared() && (_string < k._string);
}


shared_keys_t::shared_keys_t(slice_t state_data)
    : shared_keys_t()
{
    load_from(state_data);
}

shared_keys_t::shared_keys_t(const value_t *state)
    : shared_keys_t()
{
    load_from(state);
}

shared_keys_t::~shared_keys_t() = default;

size_t shared_keys_t::count() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _count;
}

bool shared_keys_t::load_from(slice_t state_data) {
    return load_from(value_t::from_data(state_data));
}

bool shared_keys_t::load_from(const value_t *state) {
    if (!state)
        return false;
    array_t::iterator i(state->as_array());
    std::lock_guard<std::mutex> lock(_mutex);
    if (i.count() <= _count)
        return false;

    i += _count;
    for (; i; ++i) {
        slice_t str = i.value()->as_string();
        if (!str)
            return false;
        int key;
        if (!shared_keys_t::_add(str, key))
            return false;
    }
    return true;
}

void shared_keys_t::set_max_key_length(size_t m) {
    _max_key_length = m;
}

bool shared_keys_t::encode(slice_t str, int &key) const {
    auto entry = _table.find(std::string(str));
    if (_usually_true(entry != _table.end())) {
        key = entry->second;
        return true;
    }
    return false;
}

bool shared_keys_t::encode_and_add(slice_t str, int &key) {
    if (encode(str, key))
        return true;
    if (str.size > _max_key_length || !is_eligible_to_encode(str))
        return false;
    std::lock_guard<std::mutex> lock(_mutex);
    if (_count >= max_count)
        return false;
    _throw_if(!_in_transaction, error_code::shared_keys_state_error, "not in transaction");
    return _add(str, key);
}

bool shared_keys_t::_add(slice_t str, int &key) {
    if (_table.find(std::string(str)) != _table.end()) {
        return false;
    }
    auto value = uint16_t(_count);
    auto entry = _table.insert_or_assign(std::string(str), value).first;
    _by_key[value] = entry->first;
    ++_count;
    key = entry->second;
    return true;
}

bool shared_keys_t::_is_unknown_key(int key) const {
    return static_cast<unsigned>(key) >= _count;
}

bool shared_keys_t::is_eligible_to_encode(slice_t str) const {
    for (size_t i = 0; i < str.size; ++i)
        if (_usually_false(!isalnum(str[i]) && str[i] != '_' && str[i] != '-'))
            return false;
    return true;
}

bool shared_keys_t::is_unknown_key(int key) const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _is_unknown_key(key);
}

bool shared_keys_t::refresh() {
    return false;
}

slice_t shared_keys_t::decode(int key) const {
    _throw_if(key < 0, error_code::invalid_data, "key must be non-negative");
    if (_usually_false(key >= static_cast<int>(max_count)))
        return null_slice;
    slice_t str = _by_key[static_cast<std::size_t>(key)];
    if (_usually_false(!str))
        return decode_unknown(key);
    return str;
}

slice_t shared_keys_t::decode_unknown(int key) const {
    const_cast<shared_keys_t*>(this)->refresh();
    std::lock_guard<std::mutex> lock(_mutex);
    return _by_key[static_cast<std::size_t>(key)];
}

std::vector<slice_t> shared_keys_t::by_key() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return std::vector<slice_t>(&_by_key[0], &_by_key[_count]);
}

shared_keys_t::platform_string_t shared_keys_t::platform_string_for_key(int key) const {
    _throw_if(key < 0, error_code::invalid_data, "key must be non-negative");
    std::lock_guard<std::mutex> lock(_mutex);
    if (unsigned(key) >= _platform_strings_by_key.size())
        return nullptr;
    return _platform_strings_by_key[static_cast<std::size_t>(key)];
}

void shared_keys_t::set_platform_string_for_key(int key, shared_keys_t::platform_string_t platformKey) const {
    std::lock_guard<std::mutex> lock(_mutex);
    _throw_if(key < 0, error_code::invalid_data, "key must be non-negative");
    _throw_if(unsigned(key) >= _count, error_code::invalid_data, "key is not yet known");
    if (unsigned(key) >= _platform_strings_by_key.size())
        _platform_strings_by_key.resize(static_cast<std::size_t>(key) + 1);
    _platform_strings_by_key[static_cast<std::size_t>(key)] = platformKey;
}

void shared_keys_t::revert_to_count(size_t count) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (count >= _count) {
        _throw_if(count > _count, error_code::shared_keys_state_error, "can't revert to a bigger count");
        return;
    }
    for (int key = int(_count - 1); key >= int(count); --key) {
        _table.erase(std::string(_by_key[static_cast<std::size_t>(key)]));
        _by_key[static_cast<std::size_t>(key)] = null_slice;
    }
    _count = unsigned(count);
}

}
