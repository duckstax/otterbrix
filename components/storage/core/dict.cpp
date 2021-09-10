#include "dict.hpp"
#include "mutable_dict.h"
#include "shared_keys.hpp"
#include "doc.hpp"
#include "internal.hpp"
#include "platform_compat.hpp"
#include <atomic>
#include <string>
#include "better_assert.hpp"

namespace storage { namespace impl {

using namespace internal;

#ifndef NDEBUG
namespace internal {
std::atomic<unsigned> total_comparisons;
bool is_disable_necessary_shared_keys_check = false;
}
static inline void count_comparison() { ++total_comparisons; }
#else
static inline void count_comparison() {}
#endif


dict_t::key_t::key_t(slice_t raw_str)
    : _raw_str(raw_str)
{}

slice_t dict_t::key_t::string() const noexcept {
    return _raw_str;
}

int dict_t::key_t::compare(const key_t &k) const noexcept {
    return _raw_str.compare(k._raw_str);
}

dict_t::key_t::~key_t() {
    release(_shared_keys); //todo
}

void dict_t::key_t::set_shared_keys(shared_keys_t *sk) {
    assert_precondition(!_shared_keys);
    _shared_keys = retain(sk);
}


template <bool WIDE>
struct dict_impl_t : public array_t::impl
{
    dict_impl_t(const dict_t *d) noexcept
        : impl(d)
    {}

    shared_keys_t* find_shared_keys() const {
        return doc_t::shared_keys(_first);
    }

    bool uses_shared_keys() const {
        return _count > 0 && _first->tag() == tag_short
                && !(dict_t::is_magic_parent_key(_first)
                     && (_count == 1 || offsetby(_first, 2*_width)->tag() != tag_short));
    }

    template <class KEY>
    const value_t* finish_get(const value_t *key_found, KEY &key_to_find) const noexcept {
        if (key_found) {
            auto value = deref(next(key_found));
            if (_usually_false(value->is_undefined()))
                value = nullptr;
            return value;
        } else {
            const dict_t *parent = get_parent();
            return parent ? parent->get(key_to_find) : nullptr;
        }
    }

    inline const value_t* get_unshared(slice_t key_to_find) const noexcept {
        auto key = search(key_to_find, [](slice_t target, const value_t *val) {
                count_comparison();
                return compare_keys(target, val);
        });
        return finish_get(key, key_to_find);
    }

    inline const value_t* get(int key_to_find) const noexcept {
        assert_precondition(key_to_find >= 0);
        auto key = search(key_to_find, [](int target, const value_t *key) {
            count_comparison();
            return compare_keys(target, key);
        });
        return finish_get(key, key_to_find);
    }

    inline const value_t* get(slice_t key_to_find, shared_keys_t *shared_keys = nullptr) const noexcept {
        if (!shared_keys && uses_shared_keys()) {
            shared_keys = find_shared_keys();
            assert_precondition(shared_keys || is_disable_necessary_shared_keys_check);
        }
        int encoded;
        if (shared_keys && lookup_shared_key(key_to_find, shared_keys, encoded))
            return get(encoded);
        return get_unshared(key_to_find);
    }

    const value_t* get(dict_t::key_t &key_to_find) const noexcept {
        auto shared_keys = key_to_find._shared_keys;
        if (!shared_keys && uses_shared_keys()) {
            shared_keys = find_shared_keys();
            key_to_find.set_shared_keys(shared_keys);
            assert_precondition(shared_keys || is_disable_necessary_shared_keys_check);
        }
        if (_usually_true(shared_keys != nullptr)) {
            if (_usually_true(key_to_find._has_numeric_key))
                return get(key_to_find._numeric_key);
            if (_usually_false(_count == 0))
                return nullptr;
            if (lookup_shared_key(key_to_find._raw_str, shared_keys, key_to_find._numeric_key)) {
                key_to_find._has_numeric_key = true;
                return get(key_to_find._numeric_key);
            }
        }
        const value_t *key = find_key_by_hint(key_to_find);
        if (!key)
            key = find_key_by_search(key_to_find);
        return finish_get(key, key_to_find);
    }

    bool has_parent() const {
        return _usually_true(_count > 0) && _usually_false(dict_t::is_magic_parent_key(_first));
    }

    const dict_t* get_parent() const {
        return has_parent() ? (const dict_t*)deref(second()) : nullptr;
    }

    static int compare_keys(slice_t key_to_find, const value_t *key) {
        if (_usually_true(key->is_int()))
            return 1;
        else
            return key_to_find.compare(key_bytes(key));
    }

    static int compare_keys(int key_to_find, const value_t *key) {
        assert_precondition(key->tag() == tag_short || key->tag() == tag_string || key->tag() >= tag_pointer);
        uint8_t byte0 = key->_byte[0];
        if (_usually_true(byte0 <= 0x07))
            return key_to_find - ((byte0 << 8) | key->_byte[1]);
        else if (_usually_false(byte0 <= 0x0F))
            return key_to_find - (int16_t)(0xF0 | (byte0 << 8) | key->_byte[1]);
        else
            return -1;
    }

    static int compare_keys(const value_t *key_to_find, const value_t *key) {
        if (key_to_find->tag() == tag_string)
            return compare_keys(key_bytes(key_to_find), key);
        else
            return compare_keys((int)key_to_find->as_int(), key);
    }

private:
    template <class T, class CMP>
    inline const value_t* search(T target, CMP comparator) const {
        const value_t *begin = _first;
        size_t n = _count;
        while (n > 0) {
            size_t mid = n >> 1;
            const value_t *mid_value = offsetby(begin, mid * 2*width);
            int cmp = comparator(target, mid_value);
            if (_usually_false(cmp == 0))
                return mid_value;
            else if (cmp < 0)
                n = mid;
            else {
                begin = offsetby(mid_value, 2*width);
                n -= mid + 1;
            }
        }
        return nullptr;
    }

    const value_t* find_key_by_hint(dict_t::key_t &key_to_find) const {
        if (key_to_find._hint < _count) {
            const value_t *key  = offsetby(_first, key_to_find._hint * 2 * width);
            if (compare_keys(key_to_find._raw_str, key) == 0)
                return key;
        }
        return nullptr;
    }

    const value_t* find_key_by_search(dict_t::key_t &key_to_find) const {
        auto key = search(key_to_find._raw_str, [](slice_t target, const value_t *val) {
                return compare_keys(target, val);
        });
        if (!key)
            return nullptr;
        key_to_find._hint = (uint32_t)index_of(key) / 2;
        return key;
    }

    bool lookup_shared_key(slice_t key_to_find, shared_keys_t *shared_keys, int &encoded) const noexcept {
        if (shared_keys->encode(key_to_find, encoded))
            return true;
        if (_count == 0)
            return false;
        const value_t *v = offsetby(_first, (_count-1)*2*width);
        do {
            if (v->is_int()) {
                if (shared_keys->is_unknown_key((int)v->as_int())) {
                    shared_keys->refresh();
                    return shared_keys->encode(key_to_find, encoded);
                }
                return false;
            }
        } while (--v >= _first);
        return false;
    }

    static inline slice_t key_bytes(const value_t *key) {
        return deref(key)->get_string_bytes();
    }

    static inline const value_t* next(const value_t *v) {
        return v->next<WIDE>();
    }

    static inline const value_t* deref(const value_t *v) {
        return v->deref<WIDE>();
    }

    static constexpr size_t width = (WIDE ? 4 : 2);
};


static int compare_keys(const value_t *key_to_find, const value_t *key, bool wide) {
    if (wide)
        return dict_impl_t<true>::compare_keys(key_to_find, key);
    else
        return dict_impl_t<true>::compare_keys(key_to_find, key);
}


constexpr dict_t::dict_t()
    : value_t(internal::tag_dict, 0, 0)
{}

uint32_t dict_t::raw_count() const noexcept {
    if (_usually_false(is_mutable()))
        return heap_dict()->count();
    return array_t::impl(this)._count;
}

uint32_t dict_t::count() const noexcept {
    if (_usually_false(is_mutable()))
        return heap_dict()->count();
    array_t::impl imp(this);
    if (_usually_false(imp._count > 1 && is_magic_parent_key(imp._first))) {
        uint32_t c = 0;
        for (iterator i(this); i; ++i)
            ++c;
        return c;
    } else {
        return imp._count;
    }
}

bool dict_t::empty() const noexcept {
    if (_usually_false(is_mutable()))
        return heap_dict()->empty();
    return count_is_zero();
}

const value_t* dict_t::get(slice_t key) const noexcept {
    if (_usually_false(is_mutable()))
        return heap_dict()->get(key);
    if (is_wide_array())
        return dict_impl_t<true>(this).get(key);
    else
        return dict_impl_t<false>(this).get(key);
}

const value_t* dict_t::get(int key) const noexcept {
    if (_usually_false(is_mutable()))
        return heap_dict()->get(key);
    else if (is_wide_array())
        return dict_impl_t<true>(this).get(key);
    else
        return dict_impl_t<false>(this).get(key);
}

const value_t* dict_t::get(key_t &key) const noexcept {
    if (_usually_false(is_mutable()))
        return heap_dict()->get(key);
    else if (is_wide_array())
        return dict_impl_t<true>(this).get(key);
    else
        return dict_impl_t<false>(this).get(key);
}

const value_t* dict_t::get(const storage::impl::key_t &key) const noexcept {
    if (_usually_false(is_mutable()))
        return heap_dict()->get(key);
    else if (key.shared())
        return get(key.as_int());
    else
        return get(key.as_string());
}

mutable_dict_t* dict_t::as_mutable() const noexcept {
    return is_mutable() ? (mutable_dict_t*)this : nullptr;
}

heap_dict_t* dict_t::heap_dict() const noexcept {
    return (heap_dict_t*)internal::heap_collection_t::as_heap_value(this);
}

const dict_t* dict_t::get_parent() const noexcept {
    if (is_mutable())
        return heap_dict()->source();
    else if (is_wide_array())
        return dict_impl_t<true>(this).get_parent();
    else
        return dict_impl_t<false>(this).get_parent();
}

bool dict_t::is_magic_parent_key(const value_t *v) {
    return v->_byte[0] == uint8_t((tag_short<<4) | 0x08) && v->_byte[1] == 0;
}

bool dict_t::is_equals(const dict_t* dv) const noexcept {
    dict_t::iterator i(this);
    dict_t::iterator j(dv);
    if (!this->get_parent() && !dv->get_parent() && i.count() != j.count())
        return false;
    if (shared_keys() == dv->shared_keys()) {
        for (; i; ++i, ++j)
            if (i.key_string() != j.key_string() || !i.value()->is_equal(j.value()))
                return false;
    } else {
        unsigned n = 0;
        for (; i; ++i, ++n) {
            auto dvalue = dv->get(i.key_string());
            if (!dvalue || !i.value()->is_equal(dvalue))
                return false;
        }
        if (dv->count() != n)
            return false;
    }
    return true;
}

EVEN_ALIGNED static constexpr dict_t empty_dict_instance;
const dict_t* const dict_t::empty_dict = &empty_dict_instance;


dict_iterator_t::dict_iterator_t(const dict_t* d) noexcept
    : dict_iterator_t(d, nullptr)
{}

dict_iterator_t::dict_iterator_t(const dict_t* d, const shared_keys_t *sk) noexcept
    : _a(d)
    , _shared_keys(sk)
{
    read();
    if (_usually_false(_key && dict_t::is_magic_parent_key(_key))) {
        _parent.reset( new dict_iterator_t(_value->as_dict()) );
        ++(*this);
    }
}

dict_iterator_t::dict_iterator_t(const dict_t* d, bool) noexcept
    : _a(d)
{
    read();
}

shared_keys_t* dict_iterator_t::find_shared_keys() const {
    auto sk = doc_t::shared_keys(_a._first);
    _shared_keys = sk;
    assert_precondition(sk || is_disable_necessary_shared_keys_check);
    return sk;
}

slice_t dict_iterator_t::key_string() const noexcept {
    slice_t key_str = _key->as_string();
    if (!key_str && _key->is_int()) {
        auto sk = _shared_keys ? _shared_keys : find_shared_keys();
        if (!sk)
            return null_slice;
        key_str = sk->decode((int)_key->as_int());
    }
    return key_str;
}

key_t dict_iterator_t::keyt() const noexcept {
    if (_key->is_int())
        return (int)_key->as_int();
    else
        return _key->as_string();
}

dict_iterator_t& dict_iterator_t::operator++() {
    do {
        if (_key_compare >= 0)
            ++(*_parent);
        if (_key_compare <= 0) {
            _throw_if(_a._count == 0, error_code::out_of_range, "iterating past end of dict");
            --_a._count;
            _a._first = offsetby(_a._first, 2*_a._width);
        }
        read();
    } while (_usually_false(_parent && _value && _value->is_undefined()));
    return *this;
}

dict_iterator_t& dict_iterator_t::operator += (uint32_t n) {
    _throw_if(n > _a._count, error_code::out_of_range, "iterating past end of dict");
    _a._count -= n;
    _a._first = offsetby(_a._first, 2*_a._width*n);
    read();
    return *this;
}

void dict_iterator_t::read() noexcept {
    if (_usually_true(_a._count)) {
        _key   = _a.deref(_a._first);
        _value = _a.deref(_a.second());
    } else {
        _key = _value = nullptr;
    }
    if (_usually_false(_parent != nullptr)) {
        auto parent_key = _parent->key();
        if (_usually_false(!_key))
            _key_compare = parent_key ? 1 : 0;
        else if (_usually_false(!parent_key))
            _key_compare = _key ? -1 : 0;
        else
            _key_compare = compare_keys(_key, parent_key, (_a._width > size_narrow));
        if (_key_compare > 0) {
            _key = parent_key;
            _value = _parent->value();
        }
    }
}

} }
