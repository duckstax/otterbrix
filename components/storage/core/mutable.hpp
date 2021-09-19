#pragma once

#include "storage.hpp"

namespace storage {

class slot_t {
public:
    void set_null()                       { slot_set_null(_slot); }
    void operator= (null_value_t)         { slot_set_null(_slot); }
    void operator= (bool v)               { slot_set_bool(_slot, v); }
    void operator= (int v)                { slot_set_int(_slot, v); }
    void operator= (unsigned v)           { slot_set_uint(_slot, v); }
    void operator= (int64_t v)            { slot_set_int(_slot, v); }
    void operator= (uint64_t v)           { slot_set_uint(_slot, v); }
    void operator= (float v)              { slot_set_float(_slot, v); }
    void operator= (double v)             { slot_set_double(_slot, v); }
    void operator= (slice_t v)            { slot_set_string(_slot, v); }
    void operator= (const char *v)        { slot_set_string(_slot, slice_t(v)); }
    void operator= (const std::string &v) { slot_set_string(_slot, slice_t(v)); }
    void operator= (value_t v)            { slot_set_value(_slot, v); }
    void operator= (std::nullptr_t)       { slot_set_value(_slot, nullptr); }
    void set_data(slice_t v)              { slot_set_data(_slot, v); }

    operator slot_t_c()                   { return _slot; }

private:
    friend class mutable_array_t;
    friend class mutable_dict_t;

    slot_t(slot_t_c slot)                 : _slot(slot) { }
    slot_t(slot_t&& slot) noexcept        : _slot(slot._slot) { }
    slot_t(const slot_t&) = delete;
    slot_t& operator=(const slot_t&) = delete;
    slot_t& operator=(slot_t&&) = delete;

    void operator= (const void*) = delete;

    slot_t_c const _slot;
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

    operator slot_t_c()                    { return _coll.set(_key); }

private:
    Collection _coll;
    Key _key;
};


class mutable_array_t : public array_t {
public:
    static mutable_array_t new_array()            { return mutable_array_t(mutable_array_new(), false); }

    mutable_array_t()                             : array_t() {}
    mutable_array_t(mutable_array_t_c a)          : array_t((array_t_c)mutable_array_retain(a)) {}
    mutable_array_t(const mutable_array_t &a)     : array_t((array_t_c)mutable_array_retain(a)) {}
    mutable_array_t(mutable_array_t &&a) noexcept : array_t((array_t_c)a) { a._val = nullptr; }
    ~mutable_array_t()                            { mutable_array_release(*this); }

    operator mutable_array_t_c () const           { return (mutable_array_t_c)_val; }

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

    array_t source() const                      { return mutable_array_get_source(*this); }
    bool is_changed() const                     { return mutable_array_is_changed(*this); }
    void remove(uint32_t first, uint32_t n = 1) { mutable_array_remove(*this, first, n); }
    void resize(uint32_t size)                  { mutable_array_resize(*this, size); }
    slot_t set(uint32_t i)                      { return slot_t(mutable_array_set(*this, i)); }
    void set_null(uint32_t i)                   { set(i).set_null(); }

    template <class T>
    void set(uint32_t i, T v)                   { set(i) = v; }

    slot_t append()                             { return mutable_array_append(*this); }
    void append_null()                          { append().set_null(); }

    template <class T>
    void append(T v)                            { append() = v; }

    void insert_nulls(uint32_t i, uint32_t n)   { mutable_array_insert(*this, i, n); }

    inline key_ref_t<mutable_array_t,uint32_t> operator[] (int i) {
        return key_ref_t<mutable_array_t,uint32_t>(*this, i);
    }

    inline value_t operator[] (int index) const { return get(index); }

    inline mutable_array_t get_mutable_array(uint32_t i);
    inline mutable_dict_t get_mutable_dict(uint32_t i);

private:
    mutable_array_t(mutable_array_t_c a, bool)  : array_t((array_t_c)a) {}

    friend class retained_value_t;
    friend class array_t;
};


class mutable_dict_t : public dict_t {
public:
    static mutable_dict_t new_dict()             { return mutable_dict_t(mutable_dict_new(), false); }

    mutable_dict_t()                             : dict_t() {}
    mutable_dict_t(mutable_dict_t_c d)           : dict_t((dict_t_c)d) { mutable_dict_retain(*this); }
    mutable_dict_t(const mutable_dict_t &d)      : dict_t((dict_t_c)d) { mutable_dict_retain(*this); }
    mutable_dict_t(mutable_dict_t &&d) noexcept  : dict_t((dict_t_c)d) { d._val = nullptr; }
    ~mutable_dict_t()                            { mutable_dict_release(*this); }

    operator mutable_dict_t_c () const           { return (mutable_dict_t_c)_val; }

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

    dict_t source() const       { return mutable_dict_get_source(*this); }
    bool is_changed() const     { return mutable_dict_is_changed(*this); }
    void remove(slice_t key)    { mutable_dict_remove(*this, key); }
    slot_t set(slice_t key)     { return mutable_dict_set(*this, key); }
    void set_null(slice_t key)  { set(key) = null_value; }
    template <class T>
    void set(slice_t key, T v)  { set(key) = v; }

    inline key_ref_t<mutable_dict_t,slice_t> operator[] (slice_t key) {
        return key_ref_t<mutable_dict_t,slice_t>(*this, key);
    }

    inline key_ref_t<mutable_dict_t,slice_t> operator[] (const char *key) {
        return key_ref_t<mutable_dict_t,slice_t>(*this, slice_t(key));
    }

    inline key_ref_t<mutable_dict_t,key_t&> operator[] (key_t &key) {
        return key_ref_t<mutable_dict_t,key_t&>(*this, key);
    }

    inline value_t operator[] (slice_t key) const      { return dict_t::get(key); }
    inline value_t operator[] (const char *key) const  { return dict_t::get(key); }

    inline mutable_array_t get_mutable_array(slice_t key);
    inline mutable_dict_t get_mutable_dict(slice_t key);

private:
    mutable_dict_t(mutable_dict_t_c d, bool)           : dict_t((dict_t_c)d) {}

    friend class retained_value_t;
    friend class dict_t;
};


class retained_value_t : public value_t {
public:
    retained_value_t() = default;
    retained_value_t(value_t_c v)                         : value_t(value_retain(v)) {}
    retained_value_t(const value_t &v)                    : value_t(value_retain(v)) {}
    retained_value_t(retained_value_t &&v) noexcept       : value_t(v) { v._val = nullptr; }
    retained_value_t(const retained_value_t &v) noexcept  : retained_value_t(value_t(v)) {}
    retained_value_t(mutable_array_t &&v) noexcept        : value_t(v) { v._val = nullptr; }
    retained_value_t(mutable_dict_t &&v) noexcept         : value_t(v) { v._val = nullptr; }
    ~retained_value_t()                                   { value_release(_val); }

    retained_value_t& operator= (const value_t &v) {
        value_retain(v);
        value_release(_val);
        _val = v;
        return *this;
    }

    retained_value_t& operator= (retained_value_t &&v) noexcept {
        if (v._val != _val) {
            value_release(_val);
            _val = v._val;
            v._val = nullptr;
        }
        return *this;
    }

    retained_value_t& operator= (std::nullptr_t) {
        value_release(_val);
        _val = nullptr;
        return *this;
    }
};


inline mutable_array_t array_t::mutable_copy(copy_flags flags) const {
    return mutable_array_t(array_mutable_copy(*this, flags), false);
}

inline mutable_dict_t dict_t::mutable_copy(copy_flags flags) const {
    return mutable_dict_t(dict_mutable_copy(*this, flags), false);
}

inline mutable_array_t mutable_array_t::get_mutable_array(uint32_t i) {
    return mutable_array_get_mutable_array(*this, i);
}

inline mutable_dict_t mutable_array_t::get_mutable_dict(uint32_t i) {
    return mutable_array_get_mutable_dict(*this, i);
}

inline mutable_array_t mutable_dict_t::get_mutable_array(slice_t key) {
    return mutable_dict_get_mutable_array(*this, key);
}

inline mutable_dict_t mutable_dict_t::get_mutable_dict(slice_t key) {
    return mutable_dict_get_mutable_dict(*this, key);
}

inline mutable_array_t array_t::as_mutable() const {
    return mutable_array_t(array_as_mutable(*this));
}

inline mutable_dict_t dict_t::as_mutable() const {
    return mutable_dict_t(dict_as_mutable(*this));
}

}
