#include "mutable_dict.hpp"
#include "mutable_dict.h"
#include <components/document/mutable/mutable_array.h>
#include <components/document/mutable/value_slot.hpp>
#include <components/document/core/shared_keys.hpp>
#include <components/document/support/better_assert.hpp>

namespace document::impl::internal {

    heap_dict_t::iterator::iterator(const heap_dict_t *dict) noexcept
        : _source_iter(dict->_source)
        , _new_iter(dict->_map.begin())
        , _new_end(dict->_map.end())
        , _count(dict->count() + 1)
        , _shared_keys(dict->shared_keys())
    {
        get_source();
        get_new();
        ++(*this);
    }

    heap_dict_t::iterator::iterator(const mutable_dict_t *dict) noexcept
        : iterator(static_cast<heap_dict_t*>(heap_collection_t::as_heap_value(dict)))
    {}

    uint32_t heap_dict_t::iterator::count() const noexcept {
        return _count;
    }

    slice_t heap_dict_t::iterator::key_string() const noexcept {
        return _key;
    }

    const value_t *heap_dict_t::iterator::value() const noexcept {
        return _value;
    }

    heap_dict_t::iterator::operator bool() const noexcept {
        return _value != nullptr;
    }

    void heap_dict_t::iterator::get_source() {
        _source_active = static_cast<bool>(_source_iter);
        if (_usually_true(_source_active))
            _source_key = key_t(_source_iter.key());
    }

    void heap_dict_t::iterator::get_new() {
        _new_active = (_new_iter != _new_end);
    }

    void heap_dict_t::iterator::decode_key(key_t key) {
        if (key.shared())
            _key = _shared_keys->decode(key.as_int());
        else
            _key = key.as_string();
    }

    heap_dict_t::iterator& heap_dict_t::iterator::operator++() {
        --_count;
        while (_usually_true(_source_active || _new_active)) {
            if (!_new_active || (_source_active && _source_key < _new_iter->first)) {
                decode_key(_source_key);
                _value = _source_iter.value();
                ++_source_iter;
                get_source();
                return *this;
            } else {
                bool exists = !!(_new_iter->second);
                if (_usually_true(exists)) {
                    decode_key(_new_iter->first);
                    _value = _new_iter->second.as_value();
                }
                if (_source_active && _source_key == _new_iter->first) {
                    ++_source_iter;
                    get_source();
                }
                ++_new_iter;
                get_new();
                if (_usually_true(exists))
                    return *this;
            }
        }
        _value = nullptr;
        return *this;
    }


    heap_dict_t::heap_dict_t(const dict_t *dict)
        : heap_collection_t(tag_dict)
    {
        if (dict) {
            _count = dict->count();
            if (dict->is_mutable()) {
                auto hd = dict->as_mutable()->heap_dict();
                _source = hd->_source;
                _map = hd->_map;
                _backing_slices = hd->_backing_slices;
            } else {
                _source = dict;
            }
            if (_source)
                _shared_keys = _source->shared_keys();
        }
    }

    mutable_dict_t *heap_dict_t::as_mutable_dict(heap_dict_t *a) {
        return static_cast<mutable_dict_t*>(const_cast<value_t*>(as_value(a)));
    }

    mutable_dict_t *heap_dict_t::as_mutable_dict() const {
        return static_cast<mutable_dict_t*>(const_cast<value_t*>(as_value()));
    }

    const dict_t *heap_dict_t::source() const {
        return _source;
    }

    shared_keys_t *heap_dict_t::shared_keys() const {
        return _shared_keys;
    }

    uint32_t heap_dict_t::count() const {
        return _count;
    }

    bool heap_dict_t::empty() const {
        return _count == 0;
    }

    void heap_dict_t::mark_changed() {
        set_changed(true);
        _iterable = nullptr;
    }

    key_t heap_dict_t::encode_key(const std::string& key) const noexcept {
        int int_key;
        if (_shared_keys && _shared_keys->encode(key, int_key))
            return key_t(int_key);
        return key_t(key);
    }

    value_slot_t* heap_dict_t::_find_value_for(const std::string& key) const noexcept {
        if (_map.empty())
            return nullptr;
        key_t encoded = encode_key(key);
        auto slot = _find_value_for(encoded);
        if (!slot && encoded.shared()) {
            slot = _find_value_for(key_t(key));
        }
        return slot;
    }

    value_slot_t* heap_dict_t::_find_value_for(key_t key) const noexcept {
        auto it = _map.find(key);
        if (it == _map.end())
            return nullptr;
        return const_cast<value_slot_t*>(&it->second);
    }

    key_t heap_dict_t::_allocate_key(key_t key) {
        if (key.shared())
            return key;
        _backing_slices.push_back(key.as_string());
        return key_t(key.as_string());
    }

    value_slot_t& heap_dict_t::_make_value_for(key_t key) {
        auto it = _map.find(key);
        if (it != _map.end())
            return it->second;
        return _map[key_t(_allocate_key(key))];
    }

    value_slot_t& heap_dict_t::setting(const std::string&key_string) {
        key_t key;
        value_slot_t *slotp = _find_value_for(key_string);
        if (slotp) {
            key = key_t(key_string);
        } else {
            key = encode_key(key_string);
            slotp = &_make_value_for(key);
        }
        if (slotp->empty() && !(_source && _source->get(key)))
            ++_count;
        mark_changed();
        return *slotp;
    }

    const value_t* heap_dict_t::get(const std::string& key) const noexcept {
        value_slot_t* val = _find_value_for(key);
        if (val)
            return val->as_value();
        else
            return _source ? _source->get(key) : nullptr;
    }

    const value_t* heap_dict_t::get(int key) const noexcept {
        auto it = _map.find(key_t(key));
        if (it != _map.end())
            return it->second.as_value();
        else
            return _source ? _source->get(key) : nullptr;
    }

    const value_t* heap_dict_t::get(dict_t::key_t &key) const noexcept {
        value_slot_t* val = _find_value_for(key.string());
        if (val)
            return val->as_value();
        else
            return _source ? _source->get(key) : nullptr;
    }

    const value_t* heap_dict_t::get(const key_t &key) const noexcept {
        auto it = _map.find(key);
        if (it != _map.end())
            return it->second.as_value();
        else
            return _source ? _source->get(key) : nullptr;
    }

    heap_collection_t* heap_dict_t::get_mutable(const std::string& str_key, tags if_type) {
        key_t key = encode_key(str_key);
        retained_t<heap_collection_t> result;
        value_slot_t* mval = _find_value_for(key);
        if (mval) {
            result = mval->make_mutable(if_type);
        } else if (_source) {
            result = heap_collection_t::mutable_copy(_source->get(key), if_type);
            if (result)
                _map.emplace(_allocate_key(key), result.get());
        }
        if (result)
            mark_changed();
        return result;
    }

    void heap_dict_t::remove(const std::string& str_key) {
        key_t key = encode_key(str_key);
        if (_source && _source->get(key)) {
            auto it = _map.find(key);
            if (it != _map.end()) {
                if (_usually_false(!it->second))
                    return;
                it->second = value_slot_t();
            } else {
                _make_value_for(key);
            }
        } else {
            if (_usually_false(!_map.erase(key)))
                return;
        }
        --_count;
        mark_changed();
    }

    void heap_dict_t::remove_all() {
        if (_count == 0)
            return;
        _map.clear();
        _backing_slices.clear();
        if (_source) {
            for (dict_t::iterator i(_source); i; ++i)
                _make_value_for(i.keyt());
        }
        _count = 0;
        mark_changed();
    }

    mutable_array_t *heap_dict_t::get_mutable_array(const std::string& key) {
        return static_cast<mutable_array_t*>(const_cast<value_t*>(as_value(get_mutable(key, tag_array))));
    }

    mutable_dict_t *heap_dict_t::get_mutable_dict(const std::string& key) {
        return static_cast<mutable_dict_t*>(const_cast<value_t*>(as_value(get_mutable(key, tag_dict))));
    }

    heap_array_t* heap_dict_t::array_key_value() {
        if (!_iterable) {
            _iterable = new heap_array_t(2*count());
            uint32_t n = 0;
            for (iterator i(this); i; ++i) {
                _iterable->set(n++, i.key_string().as_string());
                _iterable->set(n++, i.value());
            }
            assert(n == 2*_count);
        }
        return _iterable.get();
    }

    void heap_dict_t::disconnect_from_source() {
        if (!_source)
            return;
        for (dict_t::iterator i(_source); i; ++i) {
            auto key = i.key_string();
            if (_map.find(key_t(key)) == _map.end())
                set(key, i.value());
        }
        _source = nullptr;
    }

    void heap_dict_t::copy_children(copy_flags flags) {
        if (flags & copy_immutables)
            disconnect_from_source();
        for (auto &entry : _map)
            entry.second.copy_value(flags);
    }

}

namespace document::impl {

    retained_t<mutable_dict_t> mutable_dict_t::new_dict(const dict_t *d, copy_flags flags) {
        auto hd = retained(new internal::heap_dict_t(d));
        if (flags)
            hd->copy_children(flags);
        return hd->as_mutable_dict();
    }

    retained_t<mutable_dict_t> mutable_dict_t::copy(copy_flags f) {
        return new_dict(this, f);
    }

    const dict_t *mutable_dict_t::source() const {
        return heap_dict()->_source;
    }

    bool mutable_dict_t::is_changed() const {
        return heap_dict()->is_changed();
    }

    const value_t *mutable_dict_t::get(const std::string& key_to_find) const noexcept {
        return heap_dict()->get(key_to_find);
    }

    void mutable_dict_t::remove(const std::string& key) {
        heap_dict()->remove(key);
    }

    void mutable_dict_t::remove_all() {
        heap_dict()->remove_all();
    }

    mutable_array_t *mutable_dict_t::get_mutable_array(const std::string& key) {
        return heap_dict()->get_mutable_array(key);
    }

    mutable_dict_t *mutable_dict_t::get_mutable_dict(const std::string& key) {
        return heap_dict()->get_mutable_dict(key);
    }

}
