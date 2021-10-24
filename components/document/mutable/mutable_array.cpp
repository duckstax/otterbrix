#include "mutable_array.hpp"
#include "mutable_dict.hpp"
#include "mutable_array.h"
#include "varint.hpp"
#include "better_assert.hpp"

namespace storage { namespace impl {

namespace internal {

heap_array_t::iterator_t::iterator_t(const heap_array_t *ma) noexcept
    : _iter(ma->_items.begin())
    , _iter_end(ma->_items.end())
    , _source_iter(ma->_source)
{
    ++(*this);
}

heap_array_t::iterator_t::iterator_t(const mutable_array_t *ma) noexcept
    : iterator_t((heap_array_t*)heap_collection_t::as_heap_value(ma))
{}

const value_t *heap_array_t::iterator_t::value() const noexcept {
    return _value;
}

heap_array_t::iterator_t::operator bool() const noexcept {
    return _value != nullptr;
}

heap_array_t::iterator_t& heap_array_t::iterator_t::operator ++() {
    if (_iter == _iter_end) {
        _value = nullptr;
    } else {
        _value = _iter->as_value();
        if (!_value)
            _value = _source_iter[_index];
        ++_iter;
        ++_index;
    }
    return *this;
}


heap_array_t::heap_array_t()
    : heap_collection_t(tag_array)
{}

heap_array_t::heap_array_t(uint32_t initial_count)
    : heap_collection_t(tag_array)
    , _items(initial_count)
{}

heap_array_t::heap_array_t(const array_t *array)
    : heap_collection_t(tag_array)
    , _items(array ? array->count() : 0)
{
    if (array) {
        if (array->is_mutable()) {
            auto ha = array->as_mutable()->heap_array();
            _items = ha->_items;
            _source = ha->_source;
        } else {
            _source = array;
        }
    }
}

mutable_array_t *heap_array_t::as_mutable_array(heap_array_t *a) {
    return (mutable_array_t*)as_value(a);
}

mutable_array_t *heap_array_t::as_mutable_array() const {
    return (mutable_array_t*)as_value();
}

uint32_t heap_array_t::count() const {
    return (uint32_t)_items.size();
}

bool heap_array_t::empty() const {
    return _items.empty();
}

const array_t *heap_array_t::source() const {
    return _source;
}

void heap_array_t::populate(unsigned from_index) {
    if (!_source)
        return;
    auto dst = _items.begin() + from_index;
    array_t::iterator src(_source);
    for (src += from_index; src && dst != _items.end(); ++src, ++dst) {
        if (!*dst)
            dst->set(src.value());
    }
}

const value_t* heap_array_t::get(uint32_t index) {
    if (index >= count())
        return nullptr;
    auto &item = _items[index];
    if (item)
        return item.as_value();
    assert(_source);
    return _source->get(index);
}

void heap_array_t::resize(uint32_t new_size) {
    if (new_size == count())
        return;
    _items.resize(new_size, value_slot_t(null_value_t()));
    set_changed(true);
}

void heap_array_t::insert(uint32_t where, uint32_t n) {
    _throw_if(where > count(), error_code::out_of_range, "insert position is past end of array");
    if (n == 0)
        return;
    populate(where);
    _items.insert(_items.begin() + where,  n, value_slot_t(null_value_t()));
    set_changed(true);
}

void heap_array_t::remove(uint32_t where, uint32_t n) {
    _throw_if(where + n > count(), error_code::out_of_range, "remove range is past end of array");
    if (n == 0)
        return;
    populate(where + n);
    auto at = _items.begin() + where;
    _items.erase(at, at + n);
    set_changed(true);
}

heap_collection_t* heap_array_t::get_mutable(uint32_t index, tags if_type) {
    if (index >= count())
        return nullptr;
    retained_t<heap_collection_t> result = nullptr;
    auto &mval = _items[index];
    if (mval) {
        result = mval.make_mutable(if_type);
    } else if (_source) {
        result = heap_collection_t::mutable_copy(_source->get(index), if_type);
        if (result)
            _items[index].set(result->as_value());
    }
    if (result)
        set_changed(true);
    return result;
}

mutable_array_t* heap_array_t::get_mutable_array(uint32_t i)  {
    return (mutable_array_t*)as_value(get_mutable(i, internal::tag_array));
}

mutable_dict_t* heap_array_t::get_mutable_dict(uint32_t i) {
    return (mutable_dict_t*)as_value(get_mutable(i,internal::tag_dict));
}

value_slot_t& heap_array_t::setting(uint32_t index) {
#if DEBUG
    assert_precondition(index < _items.size());
#endif
    set_changed(true);
    return _items[index];
}

value_slot_t& heap_array_t::appending() {
    set_changed(true);
    _items.emplace_back();
    return _items.back();
}

value_slot_t &heap_array_t::inserting(uint32_t index) {
    insert(index, 1);
    return setting(index);
}

const value_slot_t* heap_array_t::first() {
    populate(0);
    return &_items.front();
}

void heap_array_t::disconnect_from_source() {
    if (!_source)
        return;
    uint32_t index = 0;
    for (auto &mval : _items) {
        if (!mval)
            mval.set(_source->get(index));
        ++index;
    }
    _source = nullptr;
}

void heap_array_t::copy_children(copy_flags flags) {
    if (flags & copy_immutables)
        disconnect_from_source();
    for (auto &entry : _items)
        entry.copy_value(flags);
}

} // internal


retained_t<mutable_array_t> mutable_array_t::new_array(uint32_t initial_count) {
    return (new internal::heap_array_t(initial_count))->as_mutable_array();
}

retained_t<mutable_array_t> mutable_array_t::new_array(const array_t *a, copy_flags flags) {
    auto ha = retained(new internal::heap_array_t(a));
    if (flags)
        ha->copy_children(flags);
    return ha->as_mutable_array();
}

retained_t<mutable_array_t> mutable_array_t::copy(copy_flags f) {
    return new_array(this, f);
}

const array_t *mutable_array_t::source() const {
    return heap_array()->_source;
}

bool mutable_array_t::is_changed() const {
    return heap_array()->is_changed();
}

void mutable_array_t::set_changed(bool changed) {
    heap_array()->set_changed(changed);
}

value_slot_t &mutable_array_t::setting(uint32_t index) {
    return heap_array()->setting(index);
}

value_slot_t &mutable_array_t::inserting(uint32_t index) {
    return heap_array()->inserting(index);
}

value_slot_t &mutable_array_t::appending() {
    return heap_array()->appending();
}

void mutable_array_t::resize(uint32_t new_size) {
    heap_array()->resize(new_size);
}

void mutable_array_t::insert(uint32_t where, uint32_t n) {
    heap_array()->insert(where, n);
}

void mutable_array_t::remove(uint32_t where, uint32_t n) {
    heap_array()->remove(where, n);
}

mutable_array_t *mutable_array_t::get_mutable_array(uint32_t i) {
    return heap_array()->get_mutable_array(i);
}

mutable_dict_t *mutable_array_t::get_mutable_dict(uint32_t i) {
    return heap_array()->get_mutable_dict(i);
}


} }
