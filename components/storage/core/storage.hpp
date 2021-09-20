#pragma once

#include <string>
#include <utility>
#include "storage.h"
#include "slice.hpp"

namespace storage {

class array_t;
class dict_t;
class mutable_array_t;
class mutable_dict_t;
class key_path_t;
class shared_keys_t;
class doc_t;
class encoder_t;


class value_t {
public:
    value_t() = default;
    value_t(value_t_c v)       : _val(v) {}
    operator value_t_c() const { return _val; }

    static value_t null()      { return value_t(null_value_c); }

    inline value_type type() const;
    inline bool is_integer() const;
    inline bool is_unsigned() const;
    inline bool is_double() const;
    inline bool is_mutable() const;

    inline bool as_bool() const;
    inline int64_t as_int() const;
    inline uint64_t as_unsigned() const;
    inline float as_float() const;
    inline double as_double() const;
    inline slice_t as_string() const;
    inline time_stamp as_time_stamp() const;
    inline slice_t as_data() const;
    inline array_t as_array() const;
    inline dict_t as_dict() const;

    inline alloc_slice_t to_string() const;
    inline alloc_slice_t to_json(bool canonical = false) const;
    inline std::string to_json_string() const         { return std::string(to_json()); }
    inline std::string to_std_string() const          { return as_string().as_string(); }

    explicit operator bool() const                    { return _val != nullptr; }
    bool operator! () const                           { return _val == nullptr; }
    bool operator== (value_t v) const                 { return _val == v._val; }
    bool operator== (value_t_c v) const               { return _val == v; }
    bool operator!= (value_t v) const                 { return _val != v._val; }
    bool operator!= (value_t_c v) const               { return _val != v; }

    bool is_equals(value_t v) const                   { return value_is_equals(_val, v); }

    value_t& operator= (value_t v)                    { _val = v._val; return *this; }
    value_t& operator= (std::nullptr_t)               { _val = nullptr; return *this; }

    inline value_t operator[] (const key_path_t &kp) const;

    inline doc_t find_doc() const;

    static value_t from_data(slice_t data, trust_type t = trust_type::untrusted) { return value_from_data(data, t); }

    value_t(mutable_array_t&&) = delete;
    value_t& operator= (mutable_array_t&&) = delete;
    value_t(mutable_dict_t&&) = delete;
    value_t& operator= (mutable_dict_t&&) = delete;

protected:
    value_t_c _val {nullptr};
};


class ptr_value_t {
public:
    explicit ptr_value_t(value_t v)  : _value(v) {}
    value_t* operator-> ()           { return &_value; }

private:
    value_t _value;
};


class array_t : public value_t {
public:
    class iterator_t : private array_iterator_t_c {
    public:
        inline iterator_t(array_t array)               { array_iterator_begin(array, this); }
        inline iterator_t(const array_iterator_t_c &i) : array_iterator_t_c(i) {}
        inline value_t value() const                   { return array_iterator_get_value(this); }
        inline uint32_t count() const                  { return array_iterator_get_count(this); }
        inline bool next()                             { return array_iterator_next(this); }
        inline ptr_value_t operator -> () const        { return ptr_value_t(value()); }
        inline value_t operator * () const             { return value(); }
        inline explicit operator bool() const          { return (bool)value(); }
        inline iterator_t& operator++ ()               { next(); return *this; }
        inline bool operator!= (const iterator_t&)     { return value() != nullptr; }
        inline value_t operator[] (unsigned n) const   { return array_iterator_get_value_at(this,n); }

    private:
        iterator_t() = default;

        friend class array_t;
    };


    array_t()                      : value_t() {}
    array_t(array_t_c a)           : value_t((value_t_c)a) {}
    operator array_t_c () const    { return (array_t_c)_val; }

    static array_t empty_array()   { return array_t(empty_array_c); }

    inline uint32_t count() const;
    inline bool empty() const;
    inline value_t get(uint32_t index) const;

    inline value_t operator[] (int index) const             { return get(index); }
    inline value_t operator[] (const key_path_t &kp) const  { return value_t::operator[](kp); }

    array_t& operator= (array_t a)                          { _val = a._val; return *this; }
    array_t& operator= (std::nullptr_t)                     { _val = nullptr; return *this; }
    value_t& operator= (value_t v) = delete;

    inline mutable_array_t as_mutable() const;
    inline mutable_array_t mutable_copy(copy_flags = copy_flags::default_copy) const;

    inline iterator_t begin() const                   { return iterator_t(*this); }
    inline iterator_t end() const                     { return iterator_t(); }

    array_t(mutable_array_t&&) = delete;
    array_t& operator= (mutable_array_t&&) = delete;
};


class dict_t : public value_t {
public:
    class key_t {
    public:
        explicit key_t(nonnull_slice string)          : key_t(alloc_slice_t(string)) {}
        explicit key_t(alloc_slice_t string)          : _str(std::move(string)), _key(dict_key_init(_str)) {}
        inline const alloc_slice_t& string() const    { return _str; }
        operator const alloc_slice_t&() const         { return _str; }
        operator nonnull_slice() const                { return _str; }

    private:
        alloc_slice_t _str;
        dict_key_t_c _key;

        friend class dict_t;
    };


    class iterator_t : private dict_iterator_t_c {
    public:
        inline iterator_t(dict_t d)                       { dict_iterator_begin(d, this); }
        inline iterator_t(const dict_iterator_t_c &i)     : dict_iterator_t_c(i) {}
        inline uint32_t count() const                     { return dict_iterator_get_count(this); }
        inline value_t key() const                        { return dict_iterator_get_key(this); }
        inline slice_t key_string() const                 { return dict_iterator_get_key_string(this); }
        inline value_t value() const                      { return dict_iterator_get_value(this); }
        inline bool next()                                { return dict_iterator_next(this); }
        inline ptr_value_t operator -> () const           { return ptr_value_t(value()); }
        inline value_t operator * () const                { return value(); }
        inline explicit operator bool() const             { return (bool)value(); }
        inline iterator_t& operator++ ()                  { next(); return *this; }
        inline bool operator!= (const iterator_t&) const  { return value() != nullptr; }

    private:
        iterator_t() = default;

        friend class dict_t;
    };


    dict_t()                                          : value_t() {}
    dict_t(dict_t_c d)                                : value_t((value_t_c)d) {}
    operator dict_t_c () const                        { return (dict_t_c)_val; }

    static dict_t empty_dict()                        { return dict_t(empty_dict_c); }

    inline uint32_t count() const;
    inline bool empty() const;

    inline value_t get(nonnull_slice key) const;
    inline value_t get(const char* key NONNULL) const       { return get(slice_t(key)); }

    inline value_t operator[] (nonnull_slice key) const     { return get(key); }
    inline value_t operator[] (const char *key) const       { return get(key); }
    inline value_t operator[] (const key_path_t &kp) const  { return value_t::operator[](kp); }

    dict_t& operator= (dict_t d)                            { _val = d._val; return *this; }
    dict_t& operator= (std::nullptr_t)                      { _val = nullptr; return *this; }
    value_t& operator= (value_t v) = delete;

    inline mutable_dict_t as_mutable() const;
    inline mutable_dict_t mutable_copy(copy_flags = copy_flags::default_copy) const;

    inline value_t get(key_t &key) const;
    inline value_t operator[] (key_t &key) const            { return get(key); }

    inline iterator_t begin() const                         { return iterator_t(*this); }
    inline iterator_t end() const                           { return iterator_t(); }

    dict_t(mutable_dict_t&&) = delete;
    dict_t& operator= (mutable_dict_t&&) = delete;
};


class key_path_t {
public:
    key_path_t(nonnull_slice spec, error_code *err)    : _path(key_path_new(spec, err)) {}
    ~key_path_t()                                      { key_path_free(_path); }

    key_path_t(key_path_t &&kp)                        : _path(kp._path) { kp._path = nullptr; }
    key_path_t& operator=(key_path_t &&kp)             { key_path_free(_path); _path = kp._path; kp._path = nullptr; return *this; }

    key_path_t(const key_path_t &kp)                   : key_path_t(std::string(kp), nullptr) {}

    explicit operator bool() const                     { return _path != nullptr; }
    operator key_path_t_c() const                      { return _path; }

    value_t eval(value_t root) const                   { return key_path_eval(_path, root); }
    static value_t eval(nonnull_slice specifier, value_t root, error_code *error) {
        return key_path_eval_once(specifier, root, error);
    }

    explicit operator std::string() const {
        return std::string(alloc_slice_t(key_path_to_string(_path)));
    }

    bool operator== (const key_path_t &kp) const       { return key_path_equals(_path, kp._path); }

private:
    key_path_t& operator=(const key_path_t&) = delete;

    key_path_t_c _path;

    friend class value_t;
};


class deep_iterator_t {
public:
    deep_iterator_t(value_t v)         : _i(deep_iterator_new(v)) {}
    ~deep_iterator_t()                 { deep_iterator_free(_i); }

    value_t value() const              { return deep_iterator_get_value(_i); }
    slice_t key() const                { return deep_iterator_get_key(_i); }
    uint32_t index() const             { return deep_iterator_get_index(_i); }
    value_t parent() const             { return deep_iterator_get_parent(_i); }

    size_t depth() const               { return deep_iterator_get_depth(_i); }
    alloc_slice_t path_string() const  { return deep_iterator_get_path_string(_i); }
    alloc_slice_t json_pointer() const { return deep_iterator_get_json_pointer(_i); }

    void skip_children()               { deep_iterator_skip_children(_i); }
    bool next()                        { return deep_iterator_next(_i); }

    explicit operator bool() const     { return value() != nullptr; }
    deep_iterator_t& operator++()      { next(); return *this; }

private:
    deep_iterator_t(const deep_iterator_t&) = delete;

    deep_iterator_t_c _i;
};


class shared_keys_t {
public:
    shared_keys_t()                                        : _sk(nullptr) {}
    shared_keys_t(shared_keys_t_c sk)                      : _sk(shared_keys_retain(sk)) {}
    ~shared_keys_t()                                       { shared_keys_release(_sk); }

    static shared_keys_t create()                          { return shared_keys_t(shared_keys_new(), 1); }
    static inline shared_keys_t create(slice_t state);
    bool load_state(slice_t data)                          { return shared_keys_load_state_data(_sk, data); }
    bool load_state(value_t state)                         { return shared_keys_load_state(_sk, state); }
    alloc_slice_t state_data() const                       { return shared_keys_get_state_data(_sk); }
    inline void write_state(const encoder_t &enc);
    unsigned count() const                                 { return shared_keys_count(_sk); }
    void revert_to_count(unsigned count)                   { shared_keys_revert_to_count(_sk, count); }

    operator shared_keys_t_c() const                       { return _sk; }
    bool operator== (shared_keys_t other) const            { return _sk == other._sk; }

    shared_keys_t(const shared_keys_t &other) noexcept     : _sk(shared_keys_retain(other._sk)) {}
    shared_keys_t(shared_keys_t &&other) noexcept          : _sk(other._sk) { other._sk = nullptr; }
    inline shared_keys_t& operator= (const shared_keys_t &other);
    inline shared_keys_t& operator= (shared_keys_t &&other) noexcept;

private:
    shared_keys_t(shared_keys_t_c sk, int)                 : _sk(sk) {}
    shared_keys_t_c _sk {nullptr};
};


class doc_t {
public:
    doc_t(alloc_slice_t data,
          trust_type trust = trust_type::untrusted,
          shared_keys_t sk = nullptr,
          slice_t extern_dest = null_slice) noexcept
        : _doc(doc_from_result_data(slice_result_t_c(std::move(data)), trust, sk, extern_dest))
    {}

    static inline doc_t from_json(nonnull_slice json, error_code *out_error = nullptr);

    static alloc_slice_t dump(nonnull_slice ata)     { return data_dump(ata); }

    doc_t()                                          : _doc(nullptr) {}
    doc_t(doc_t_c d, bool retain = true)             : _doc(d) { if (retain) doc_retain(_doc); }
    doc_t(const doc_t &other) noexcept               : _doc(doc_retain(other._doc)) {}
    doc_t(doc_t &&other) noexcept                    : _doc(other._doc) {other._doc = nullptr; }
    doc_t& operator=(const doc_t &other);
    doc_t& operator=(doc_t &&other) noexcept;
    ~doc_t()                                         { doc_release(_doc); }

    slice_t data() const                             { return doc_get_data(_doc); }
    alloc_slice_t alloced_data() const               { return doc_get_alloced_data(_doc); }
    shared_keys_t shared_keys() const                { return doc_get_shared_keys(_doc); }

    value_t root() const                             { return doc_get_root(_doc); }
    explicit operator bool () const                  { return root() != nullptr; }
    array_t as_array() const                         { return root().as_array(); }
    dict_t as_dict() const                           { return root().as_dict(); }

    operator value_t () const                        { return root(); }
    operator dict_t () const                         { return as_dict(); }
    operator dict_t_c () const                       { return as_dict(); }

    value_t operator[] (int index) const             { return as_array().get(index); }
    value_t operator[] (slice_t key) const           { return as_dict().get(key); }
    value_t operator[] (const char *key) const       { return as_dict().get(key); }
    value_t operator[] (const key_path_t &kp) const  { return root().operator[](kp); }

    bool operator== (const doc_t &d) const           { return _doc == d._doc; }

    operator doc_t_c() const                         { return _doc; }
    doc_t_c detach()                                 { auto d = _doc; _doc = nullptr; return d; }

    static doc_t containing(value_t v)               { return doc_t(value_find_doc(v), false); }
    bool set_associated(void *p, const char *t)      { return doc_set_associated(_doc, p, t); }
    void* associated(const char *type) const         { return doc_get_associated(_doc, type); }

private:
    explicit doc_t(value_t_c v)                      : _doc(value_find_doc(v)) {}

    doc_t_c _doc;

    friend class value_t;
};


class null_value_t {};
constexpr null_value_t null_value;


class encoder_t {
public:
    class key_ref_t {
    public:
        key_ref_t(encoder_t &enc, slice_t key)  : _enc(enc), _key(key) {}
        template <class T>
        inline void operator= (T value)         { _enc.write_key(_key); _enc << value; }

    private:
        encoder_t &_enc;
        slice_t _key;
    };


    encoder_t()  : _enc(encoder_new()) {}

    explicit encoder_t(encode_format format, size_t reserveSize = 0, bool unique_strings = true)
        : _enc(encoder_new_with_options(format, reserveSize, unique_strings))
    {}

    explicit encoder_t(FILE *file, bool unique_strings = true)
        : _enc(encoder_new_writing_to_file(file, unique_strings))
    {}

    explicit encoder_t(shared_keys_t sk)                : encoder_t() { set_shared_keys(sk); }
    explicit encoder_t(encoder_t_c enc)                 : _enc(enc) {}
    encoder_t(encoder_t&& enc)                          : _enc(enc._enc) { enc._enc = nullptr; }

    ~encoder_t()                                        { encoder_free(_enc); }

    void detach()                                       { _enc = nullptr; }
    void set_shared_keys(shared_keys_t sk)              { encoder_set_shared_keys(_enc, sk); }
    inline void amend(slice_t base, bool reuse_strings = false, bool extern_pointers = false);
    slice_t base() const                                { return encoder_get_base(_enc); }
    void suppress_trailer()                             { encoder_suppress_trailer(_enc); }

    operator encoder_t_c () const                       { return _enc; }

    inline bool write_null();
    inline bool write_undefined();
    inline bool write_bool(bool value);
    inline bool write_int(int64_t value);
    inline bool write_uint(uint64_t value);
    inline bool write_float(float value);
    inline bool write_double(double value);
    inline bool write_string(slice_t value);
    inline bool write_string(const char *s);
    inline bool write_string(std::string s);
    inline bool write_date_string(time_stamp ts, bool utc = true);
    inline bool write_data(slice_t value);
    inline bool write_value(value_t value);
    inline bool convert_json(nonnull_slice j);
    inline bool begin_array(size_t reserve_count = 0);
    inline bool end_array();
    inline bool begin_dict(size_t reserve_count = 0);
    inline bool write_key(nonnull_slice key);
    inline bool write_key(value_t key);
    inline bool end_dict();

    template <class T>
    inline void write(nonnull_slice key, T value)    { write_key(key); *this << value; }
    inline void write_raw(slice_t data)              { encoder_write_raw(_enc, data); }

    inline size_t bytes_written_size() const;
    inline size_t next_write_pos() const             { return encoder_get_next_write_pos(_enc); }

    inline doc_t finish_doc(error_code *error = nullptr);
    inline alloc_slice_t finish(error_code *error = nullptr);
    inline size_t finish_item()                      { return encoder_finish_item(_enc); }
    inline void reset();

    inline error_code error() const;
    inline const char* error_message() const;

    encoder_t& operator<< (null_value_t)             { write_null(); return *this; }
    encoder_t& operator<< (long long i)              { write_int(i); return *this; }
    encoder_t& operator<< (unsigned long long i)     { write_uint(i); return *this; }
    encoder_t& operator<< (long i)                   { write_int(i); return *this; }
    encoder_t& operator<< (unsigned long i)          { write_uint(i); return *this; }
    encoder_t& operator<< (int i)                    { write_int(i); return *this; }
    encoder_t& operator<< (unsigned int i)           { write_uint(i); return *this; }
    encoder_t& operator<< (double d)                 { write_double(d); return *this; }
    encoder_t& operator<< (float f)                  { write_float(f); return *this; }
    encoder_t& operator<< (slice_t s)                { write_string(s); return *this; }
    encoder_t& operator<< (const  char *str)         { write_string(str); return *this; }
    encoder_t& operator<< (const std::string &s)     { write_string(s); return *this; }
    encoder_t& operator<< (value_t v)                { write_value(v); return *this; }

    inline key_ref_t operator[] (nonnull_slice key)  { return key_ref_t(*this, key); }

protected:
    encoder_t(const encoder_t&) = delete;
    encoder_t& operator=(const encoder_t&) = delete;

    encoder_t_c _enc;
};


class json_encoder_t : public encoder_t {
public:
    json_encoder_t()                     : encoder_t(encode_format::json) {}
    inline bool write_raw(slice_t raw)   { return encoder_write_raw(_enc, raw); }
};


class shared_encoder_t : public encoder_t {
public:
    explicit shared_encoder_t(encoder_t_c enc)  : encoder_t(enc) {}
    ~shared_encoder_t()                         { detach(); }
};


class json_delta_t {
public:
    static inline alloc_slice_t create(value_t old, value_t nuu);
    static inline bool create(value_t old, value_t nuu, encoder_t &json_encoder);
    static inline alloc_slice_t apply(value_t old, slice_t json_delta, error_code *error);
    static inline bool apply(value_t old, slice_t json_delta, encoder_t &encoder);
};


class alloced_dict_t : public dict_t, alloc_slice_t {
public:
    alloced_dict_t() = default;

    explicit alloced_dict_t(alloc_slice_t s)
        : dict_t(value_as_dict(value_from_data(s, trust_type::untrusted)))
        , alloc_slice_t(std::move(s))
    {}

    explicit alloced_dict_t(slice_t s)                 : alloced_dict_t(alloc_slice_t(s)) {}

    const alloc_slice_t& data() const                  { return *this; }
    explicit operator bool () const                    { return dict_t::operator bool(); }

    inline value_t operator[] (slice_t key) const      { return dict_t::get(key); }
    inline value_t operator[] (const char *key) const  { return dict_t::get(key); }
};


inline value_type value_t::type() const           { return value_get_type(_val); }
inline bool value_t::is_integer() const           { return value_is_integer(_val); }
inline bool value_t::is_unsigned() const          { return value_is_unsigned(_val); }
inline bool value_t::is_double() const            { return value_is_double(_val); }
inline bool value_t::is_mutable() const           { return value_is_mutable(_val); }

inline bool value_t::as_bool() const              { return value_as_bool(_val); }
inline int64_t value_t::as_int() const            { return value_as_int(_val); }
inline uint64_t value_t::as_unsigned() const      { return value_as_unsigned(_val); }
inline float value_t::as_float() const            { return value_as_float(_val); }
inline double value_t::as_double() const          { return value_as_double(_val); }
inline time_stamp value_t::as_time_stamp() const  { return value_as_timestamp(_val); }
inline slice_t value_t::as_string() const         { return value_as_string(_val); }
inline slice_t value_t::as_data() const           { return value_as_data(_val); }
inline array_t value_t::as_array() const          { return value_as_array(_val); }
inline dict_t value_t::as_dict() const            { return value_as_dict(_val); }

inline alloc_slice_t value_t::to_string() const   { return value_to_string(_val); }

inline alloc_slice_t value_t::to_json(bool canonical) const {
    return value_to_json(_val, canonical);
}

inline value_t value_t::operator[] (const key_path_t &kp) const {
    return key_path_eval(kp._path, _val);
}

inline doc_t value_t::find_doc() const            { return doc_t(_val); }
inline uint32_t array_t::count() const            { return array_count(*this); }
inline bool array_t::empty() const                { return array_is_empty(*this); }
inline value_t array_t::get(uint32_t i) const     { return array_get(*this, i); }

inline uint32_t dict_t::count() const                 { return dict_count(*this); }
inline bool dict_t::empty() const                     { return dict_is_empty(*this); }
inline value_t dict_t::get(nonnull_slice key) const   { return dict_get(*this, key); }
inline value_t dict_t::get(dict_t::key_t &key) const  { return dict_get_with_key(*this, &key._key); }

inline void shared_keys_t::write_state(const encoder_t &enc)  { shared_keys_write_state(_sk, enc); }

inline void encoder_t::amend(slice_t base, bool reuse_strings, bool extern_pointers) {
    encoder_amend(_enc, base, reuse_strings, extern_pointers);
}

inline bool encoder_t::write_null()                               { return encoder_write_null(_enc); }
inline bool encoder_t::write_undefined()                          { return encoder_write_undefined(_enc); }
inline bool encoder_t::write_bool(bool b)                         { return encoder_write_bool(_enc, b); }
inline bool encoder_t::write_int(int64_t n)                       { return encoder_write_int(_enc, n); }
inline bool encoder_t::write_uint(uint64_t n)                     { return encoder_write_uint(_enc, n); }
inline bool encoder_t::write_float(float n)                       { return encoder_write_float(_enc, n); }
inline bool encoder_t::write_double(double n)                     { return encoder_write_double(_enc, n); }
inline bool encoder_t::write_string(slice_t s)                    { return encoder_write_string(_enc, s); }
inline bool encoder_t::write_string(const char *s)                { return write_string(slice_t(s)); }
inline bool encoder_t::write_string(std::string s)                { return write_string(slice_t(s)); }
inline bool encoder_t::write_date_string(time_stamp ts, bool utc) { return encoder_write_date_string(_enc, ts, utc); }
inline bool encoder_t::write_data(slice_t data)                   { return encoder_write_data(_enc, data); }
inline bool encoder_t::write_value(value_t v)                     { return encoder_write_value(_enc, v); }
inline bool encoder_t::convert_json(nonnull_slice j)              { return encoder_convert_json(_enc, j); }
inline bool encoder_t::begin_array(size_t reserve_count)          { return encoder_begin_array(_enc, reserve_count); }
inline bool encoder_t::end_array()                                { return encoder_end_array(_enc); }
inline bool encoder_t::begin_dict(size_t reserve_count)           { return encoder_begin_dict(_enc, reserve_count); }
inline bool encoder_t::write_key(nonnull_slice key)               { return encoder_write_key(_enc, key); }
inline bool encoder_t::write_key(value_t key)                     { return encoder_write_key_value(_enc, key); }
inline bool encoder_t::end_dict()                                 { return encoder_end_dict(_enc); }
inline size_t encoder_t::bytes_written_size() const               { return encoder_bytes_written(_enc); }
inline doc_t encoder_t::finish_doc(error_code* err)               { return doc_t(encoder_finish_doc(_enc, err), false); }
inline alloc_slice_t encoder_t::finish(error_code* err)           { return encoder_finish(_enc, err); }
inline void encoder_t::reset()                                    { return encoder_reset(_enc); }
inline error_code encoder_t::error() const                        { return encoder_get_error(_enc); }
inline const char* encoder_t::error_message() const               { return encoder_get_error_message(_enc); }

template<>
inline void encoder_t::key_ref_t::operator= (bool value)          { _enc.write_key(_key); _enc.write_bool(value); }

inline alloc_slice_t json_delta_t::create(value_t old, value_t nuu) {
    return create_json_delta(old, nuu);
}

inline bool json_delta_t::create(value_t old, value_t nuu, encoder_t &json_encoder) {
    return encode_json_delta(old, nuu, json_encoder);
}

inline alloc_slice_t json_delta_t::apply(value_t old, slice_t json_delta, error_code *error) {
    return apply_json_delta(old, json_delta, error);
}

inline bool json_delta_t::apply(value_t old, slice_t json_delta, encoder_t &encoder) {
    return encode_applying_json_delta(old, json_delta, encoder);
}

inline shared_keys_t shared_keys_t::create(slice_t state) {
    auto sk = create();
    sk.load_state(state);
    return sk;
}

inline shared_keys_t& shared_keys_t::operator= (const shared_keys_t &other) {
    auto sk = shared_keys_retain(other._sk);
    shared_keys_release(_sk);
    _sk = sk;
    return *this;
}

inline shared_keys_t& shared_keys_t::operator= (shared_keys_t &&other) noexcept {
    shared_keys_release(_sk);
    _sk = other._sk;
    other._sk = nullptr;
    return *this;
}

inline doc_t doc_t::from_json(nonnull_slice json, error_code *out_error) {
    return doc_t(doc_from_json(json, out_error), false);
}

inline doc_t& doc_t::operator=(const doc_t &other) {
    if (other._doc != _doc) {
        doc_release(_doc);
        _doc = doc_retain(other._doc);
    }
    return *this;
}

inline doc_t& doc_t::operator=(doc_t &&other) noexcept {
    if (other._doc != _doc) {
        doc_release(_doc);
        _doc = other._doc;
        other._doc = nullptr;
    }
    return *this;
}

}
