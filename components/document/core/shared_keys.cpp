#include "shared_keys.hpp"
#include <components/document/support/exception.hpp>
#include <components/document/core/value.hpp>
#include <components/document/core/array.hpp>

namespace document::impl {

key_t::key_t(std::string_view key)
    : _string({key.data(),key.size()})
{
    assert_precondition(!key.empty());
}

key_t::key_t(const std::string& key)
    : _string(key)
{
    assert_precondition(!key.empty());
}

key_t::key_t(int key)
    : _int(static_cast<int16_t>(key))
{
    assert_precondition(key >= 0 && key <= INT16_MAX);
}

key_t::key_t(const value_t *v) noexcept {
    if (v->is_int()) {
        _int = static_cast<int16_t>(v->as_int());
    } else {
        _string = v->as_string();
    }
}

bool key_t::shared() const {
    return _string.empty();
}

int key_t::as_int() const {
    assert_precondition(shared());
    return _int;
}

std::string_view key_t::as_string() const {
    return {_string.data(),_string.size()};
}

bool key_t::operator== (const key_t &k) const noexcept {
    return shared() ? (_int == k._int) : (_string == k._string);
}

bool key_t::operator<(const key_t& k) const noexcept {
    if (shared()) {
        return !k.shared() || (_int < k._int);
    } else {
        return !k.shared() && (_string < k._string);
    }
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


bool shared_keys_t::load_from(const value_t *state) {
    if (!state)
        return false;
    array_t::iterator i(state->as_array());
    std::lock_guard<std::mutex> lock(_mutex);
    if (i.count() <= _count)
        return false;

    i += _count;
    for (; i; ++i) {
        auto tmp =  i.value()->as_string();
        std::string str ( tmp.data(),tmp.size()); //todo: string_view;
        if (!str.empty())
            return false;
        int key;
        if (!shared_keys_t::_add(str, key))
            return false;
    }
    return true;
}

bool shared_keys_t::encode(const std::string& str, int &key) const {
    auto entry = _table.find(str);
    if (_usually_true(entry != _table.end())) {
        key = entry->second;
        return true;
    }
    return false;
}

bool shared_keys_t::encode(std::string_view str, int &key) const {
    std::string tmp(str.data(),str.size()); /// todo refatorimg;
    auto entry = _table.find(tmp);
    if (_usually_true(entry != _table.end())) {
        key = entry->second;
        return true;
    }
    return false;
}

bool shared_keys_t::encode_and_add(const std::string& str, int &key) {
    if (encode(str, key))
        return true;
    if (str.length() > _max_key_length || !is_eligible_to_encode(str))
        return false;
    std::lock_guard<std::mutex> lock(_mutex);
    if (_count >= max_count)
        return false;
    _throw_if(!_in_transaction, error_code::shared_keys_state_error, "not in transaction");
    return _add(str, key);
}

bool shared_keys_t::_add(const std::string& str, int &key) {
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

bool shared_keys_t::is_eligible_to_encode(const std::string& str) const {
    for (size_t i = 0; i < str.length(); ++i)
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

std::string shared_keys_t::decode(int key) const {
    _throw_if(key < 0, error_code::invalid_data, "key must be non-negative");
    if (_usually_false(key >= static_cast<int>(max_count)))
        return {}; ///todo: string_view
    auto str = _by_key[static_cast<std::size_t>(key)];
    if (_usually_false(!str.empty()))
        return decode_unknown(key);
    return str;
}

std::string shared_keys_t::decode_unknown(int key) const {
    const_cast<shared_keys_t*>(this)->refresh();
    std::lock_guard<std::mutex> lock(_mutex);
    return _by_key[static_cast<std::size_t>(key)];
}

std::vector<std::string> shared_keys_t::by_key() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return std::vector<std::string>(&_by_key[0], &_by_key[_count]);
}

void shared_keys_t::revert_to_count(size_t count) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (count >= _count) {
        _throw_if(count > _count, error_code::shared_keys_state_error, "can't revert to a bigger count");
        return;
    }
    for (int key = int(_count - 1); key >= int(count); --key) {
        _table.erase(std::string(_by_key[static_cast<std::size_t>(key)]));
        _by_key[static_cast<std::size_t>(key)] = {};
    }
    _count = unsigned(count);
}

}
