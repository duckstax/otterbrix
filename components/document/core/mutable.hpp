#pragma once

#include "storage.hpp"
#include "mutable_array.h"
#include "mutable_dict.h"
#include "array.hpp"
#include "dict.hpp"

namespace storage {

inline impl_mutable_array_t mutable_array_new() noexcept;
inline impl_mutable_dict_t mutable_dict_new() noexcept;
inline impl_mutable_array_t mutable_array_retain(impl_mutable_array_t a);
inline impl_mutable_dict_t mutable_dict_retain(impl_mutable_dict_t d);
inline void mutable_array_release(impl_mutable_array_t a);
inline void mutable_dict_release(impl_mutable_dict_t d);

class slot_t {
public:
    void set_null()                       { _slot->set(null_value); }
    void operator= (null_value_t)         { _slot->set(null_value); }
    void operator= (bool v)               { _slot->set(v); }
    void operator= (int v)                { _slot->set(v); }
    void operator= (unsigned v)           { _slot->set(v); }
    void operator= (int64_t v)            { _slot->set(v); }
    void operator= (uint64_t v)           { _slot->set(v); }
    void operator= (float v)              { _slot->set(v); }
    void operator= (double v)             { _slot->set(v); }
    void operator= (slice_t v)            { _slot->set(v); }
    void operator= (const char *v)        { _slot->set(slice_t(v)); }
    void operator= (const std::string &v) { _slot->set(slice_t(v)); }
    void operator= (value_t v)            { _slot->set(v); }
    void operator= (std::nullptr_t)       { _slot->set(nullptr); }
    void set_data(slice_t v)              { _slot->set_data(v); }

    operator impl_slot_t()                { return _slot; }

private:
    friend class mutable_array_t;
    friend class mutable_dict_t;

    slot_t(impl_slot_t slot)                 : _slot(slot) { }
    slot_t(slot_t&& slot) noexcept           : _slot(slot._slot) { }
    slot_t(const slot_t&) = delete;
    slot_t& operator=(const slot_t&) = delete;
    slot_t& operator=(slot_t&&) = delete;

    void operator= (const void*) = delete;

    impl_slot_t const _slot;
};


template <class Collection, class Key>
class key_ref_t : public value_t {
public:
    key_ref_t(Collection &coll, Key key)   : value_t(coll.get(key)), _coll(coll), _key(key) {}
    template <class T>
    void operator= (const key_ref_t &ref)  { _coll.set(_key, ref); }
    template <class T>
    void operator= (const T &value)        { _coll.set(_key, value); }
    void set_data(slice_t value)           { _coll.set(_key).set_data(value); }
    void remove()                          { _coll.remove(_key); }

    operator impl_slot_t()                 { return _coll.set(_key); }

private:
    Collection _coll;
    Key _key;
};


class mutable_array_t : public array_t {
public:
    static mutable_array_t new_array()            { return mutable_array_t(mutable_array_new(), false); }

    mutable_array_t()                             : array_t() {}
    mutable_array_t(impl_mutable_array_t a)       : array_t(mutable_array_retain(a)) {}
    mutable_array_t(const mutable_array_t &a)     : array_t(mutable_array_retain(a)) {}
    mutable_array_t(mutable_array_t &&a) noexcept : array_t(static_cast<impl_array_t>(a)) { a._val = nullptr; }
    ~mutable_array_t()                            { mutable_array_release(*this); }

    operator impl_mutable_array_t () const        { return impl_mutable_array_t(_val); }

    mutable_array_t& operator= (const mutable_array_t &a) {
        mutable_array_retain(a);
        mutable_array_release(*this);
        _val = a._val;
        return *this;
    }

    mutable_array_t& operator= (mutable_array_t &&a) noexcept {
        if (a._val != _val) {
            mutable_array_release(*this);
            _val = a._val;
            a._val = nullptr;
        }
        return *this;
    }

    array_t source() const                      { return _ima()->source(); }
    bool is_changed() const                     { return _ima()->is_changed(); }
    void remove(uint32_t first, uint32_t n = 1) { _ima()->remove(first, n); }
    void resize(uint32_t size)                  { _ima()->resize(size); }
    slot_t set(uint32_t i)                      { return slot_t(&_ima()->setting(i)); }
    void set_null(uint32_t i)                   { set(i).set_null(); }

    template <class T>
    void set(uint32_t i, T v)                   { set(i) = v; }

    slot_t append()                             { return &_ima()->appending(); }
    void append_null()                          { append().set_null(); }

    template <class T>
    void append(T v)                            { append() = v; }

    void insert_nulls(uint32_t i, uint32_t n)   { _ima()->insert(i, n); }

    inline key_ref_t<mutable_array_t, uint32_t> operator[] (int i) {
        return key_ref_t<mutable_array_t, uint32_t>(*this, uint32_t(i));
    }

    inline value_t operator[] (int index) const { return get(static_cast<uint32_t>(index)); }

    inline mutable_array_t get_mutable_array(uint32_t i);
    inline mutable_dict_t get_mutable_dict(uint32_t i);

private:
    mutable_array_t(impl_mutable_array_t a, bool)  : array_t(static_cast<impl_array_t>(a)) {}

    impl_array_t _ia() const          { return _val->as_array(); }
    impl_mutable_array_t _ima() const { return _ia()->as_mutable(); }

    friend class retained_value_t;
    friend class array_t;
};


class mutable_dict_t : public dict_t {
public:
    static mutable_dict_t new_dict()             { return mutable_dict_t(mutable_dict_new(), false); }

    mutable_dict_t()                             : dict_t() {}
    mutable_dict_t(impl_mutable_dict_t d)        : dict_t(mutable_dict_retain(d)) {}
    mutable_dict_t(const mutable_dict_t &d)      : dict_t(mutable_dict_retain(d)) {}
    mutable_dict_t(mutable_dict_t &&d) noexcept  : dict_t(mutable_dict_retain(d)) { d._val = nullptr; }
    ~mutable_dict_t()                            { mutable_dict_release(*this); }

    operator impl_mutable_dict_t () const        { return impl_mutable_dict_t(_val); }

    mutable_dict_t& operator= (const mutable_dict_t &d) {
        mutable_dict_retain(d);
        mutable_dict_release(*this);
        _val = d._val;
        return *this;
    }

    mutable_dict_t& operator= (mutable_dict_t &&d) noexcept {
        if (d._val != _val) {
            mutable_dict_release(*this);
            _val = d._val;
            d._val = nullptr;
        }
        return *this;
    }

    dict_t source() const       { return _imd()->source(); }
    bool is_changed() const     { return _imd()->is_changed(); }
    void remove(slice_t key)    { _imd()->remove(key); }
    slot_t set(slice_t key)     { return slot_t(&_imd()->setting(key)); }
    void set_null(slice_t key)  { set(key).set_null(); }
    template <class T>
    void set(slice_t key, T v)  { set(key) = v; }

    inline key_ref_t<mutable_dict_t, slice_t> operator[] (slice_t key) {
        return key_ref_t<mutable_dict_t, slice_t>(*this, key);
    }

    inline key_ref_t<mutable_dict_t, slice_t> operator[] (const char *key) {
        return key_ref_t<mutable_dict_t, slice_t>(*this, slice_t(key));
    }

    inline key_ref_t<mutable_dict_t, key_t&> operator[] (key_t &key) {
        return key_ref_t<mutable_dict_t, key_t&>(*this, key);
    }

    inline value_t operator[] (slice_t key) const      { return dict_t::get(key); }
    inline value_t operator[] (const char *key) const  { return dict_t::get(key); }

    inline mutable_array_t get_mutable_array(slice_t key);
    inline mutable_dict_t get_mutable_dict(slice_t key);

private:
    mutable_dict_t(impl_mutable_dict_t d, bool) : dict_t(static_cast<impl_dict_t>(d)) {}

    impl_dict_t _id() const          { return _val->as_dict(); }
    impl_mutable_dict_t _imd() const { return _id()->as_mutable(); }

    friend class retained_value_t;
    friend class dict_t;
};


class retained_value_t : public value_t {
public:
    retained_value_t() = default;
    retained_value_t(impl_value_t v)                      : value_t(retain(v)) {}
    retained_value_t(const value_t &v)                    : value_t(retain(static_cast<impl_value_t>(v))) {}
    retained_value_t(retained_value_t &&v) noexcept       : value_t(v) { v._val = nullptr; }
    retained_value_t(const retained_value_t &v) noexcept  : retained_value_t(value_t(v)) {}
    retained_value_t(mutable_array_t &&v) noexcept        : value_t(v) { v._val = nullptr; }
    retained_value_t(mutable_dict_t &&v) noexcept         : value_t(v) { v._val = nullptr; }
    ~retained_value_t()                                   { release(_val); }

    retained_value_t& operator= (const value_t &v) {
        retain(static_cast<impl_value_t>(v));
        release(_val);
        _val = v;
        return *this;
    }

    retained_value_t& operator= (retained_value_t &&v) noexcept {
        if (v._val != _val) {
            release(_val);
            _val = v._val;
            v._val = nullptr;
        }
        return *this;
    }

    retained_value_t& operator= (std::nullptr_t) {
        release(_val);
        _val = nullptr;
        return *this;
    }
};


inline impl_mutable_array_t _new_mutable_array(impl_array_t a, copy_flags flags) noexcept {
    try {
        return retain(impl::mutable_array_t::new_array(a, flags));
    } catch (const std::exception &x) {
        storage::impl::record_error(x, nullptr);
    }
    return nullptr;
}

inline impl_mutable_dict_t _new_mutable_dict(impl_dict_t d, copy_flags flags) noexcept {
    try {
        return retain(impl::mutable_dict_t::new_dict(d, copy_flags(flags)));
    } catch (const std::exception &x) {
        storage::impl::record_error(x, nullptr);
    }
    return nullptr;
}

inline impl_mutable_array_t mutable_array_new() noexcept {
    return _new_mutable_array(nullptr, copy_flags::deep_copy);
}

inline impl_mutable_dict_t mutable_dict_new() noexcept {
    return _new_mutable_dict(nullptr, copy_flags::default_copy);
}

inline impl_mutable_array_t mutable_array_retain(impl_mutable_array_t a) {
    return impl_mutable_array_t(retain(static_cast<impl_value_t>(a)));
}

inline impl_mutable_dict_t mutable_dict_retain(impl_mutable_dict_t d) {
    return impl_mutable_dict_t(retain(static_cast<impl_value_t>(d)));
}

inline void mutable_array_release(impl_mutable_array_t a) {
    release(static_cast<impl_value_t>(a));
}

inline void mutable_dict_release(impl_mutable_dict_t d) {
    release(static_cast<impl_value_t>(d));
}

inline mutable_array_t array_t::mutable_copy(copy_flags flags) const {
    return mutable_array_t(_new_mutable_array(static_cast<impl_array_t>(*this), flags), false);
}

inline mutable_dict_t dict_t::mutable_copy(copy_flags flags) const {
    return mutable_dict_t(_new_mutable_dict(static_cast<impl_dict_t>(*this), flags), false);
}

inline mutable_array_t mutable_array_t::get_mutable_array(uint32_t i) {
    return _ima()->get_mutable_array(i);
}

inline mutable_dict_t mutable_array_t::get_mutable_dict(uint32_t i) {
    return _ima()->get_mutable_dict(i);
}

inline mutable_array_t mutable_dict_t::get_mutable_array(slice_t key) {
    return _imd()->get_mutable_array(key);
}

inline mutable_dict_t mutable_dict_t::get_mutable_dict(slice_t key) {
    return _imd()->get_mutable_dict(key);
}

inline mutable_array_t array_t::as_mutable() const {
    return static_cast<impl_array_t>(*this)->as_mutable();
}

inline mutable_dict_t dict_t::as_mutable() const {
    return static_cast<impl_dict_t>(*this)->as_mutable();
}

}
