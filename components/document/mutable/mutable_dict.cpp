#include "mutable_dict.hpp"
#include "mutable_dict.h"
#include <components/document/core/array.hpp>
#include <components/document/internal/value_slot.hpp>
#include <components/document/core/shared_keys.hpp>
#include <components/document/support/better_assert.hpp>

namespace document::impl::internal {

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
    }

    key_t heap_dict_t::encode_key(const std::string& key) const noexcept {
        int int_key = 0;
        if (_shared_keys && _shared_keys->encode(key, int_key))
            return key_t(int_key);
        return key_t(key);
    }

    key_t heap_dict_t::encode_key(std::string_view key) const noexcept {
        int int_key = 0;
        if (_shared_keys && _shared_keys->encode(key, int_key))
            return key_t(int_key);
        return key_t(key);
    }


    value_slot_t* heap_dict_t::_find_value_for(std::string_view key) const noexcept {
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
        auto tmp_key = key.as_string();
        _backing_slices.emplace_back(tmp_key.data(),tmp_key.size());
        return key_t(tmp_key);
    }

    value_slot_t& heap_dict_t::_make_value_for(key_t key) {
        auto it = _map.find(key);
        if (it != _map.end())
            return it->second;
        return _map[key_t(_allocate_key(key))];
    }

    value_slot_t& heap_dict_t::setting(std::string_view key_string) {
        key_t key;
        value_slot_t* slotp = _find_value_for(key_string);
        if (slotp) {
            key = key_t(key_string);
        } else {
            key = encode_key(key_string);
            slotp = &_make_value_for(key);
        }
        if (slotp->empty() && !(_source && _source->get(key))) {
            ++_count;
        }
        mark_changed();
        return *slotp;
    }

    const value_t* heap_dict_t::get(std::string_view key) const noexcept {
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

    array_t* heap_dict_t::array_key_value() {
        auto *array = array_t::new_array(2 * count()).detach();
        uint32_t n = 0;
        for (auto value : _map) {
            array->set(n++, value.first.as_string());
            array->set(n++, value.second.as_value());
        }
        return array;
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

    const value_t *mutable_dict_t::get(std::string_view key_to_find) const noexcept {
        return heap_dict()->get(key_to_find);
    }

    void mutable_dict_t::remove(const std::string& key) {
        heap_dict()->remove(key);
    }

    void mutable_dict_t::remove_all() {
        heap_dict()->remove_all();
    }

}
