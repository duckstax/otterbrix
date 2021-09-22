#pragma once

#include <string>
#include <utility>
#include "storage_impl.hpp"
#include "slice_core.hpp"
#include "value_slot.hpp"
#include "path.hpp"
#include "deep_iterator.hpp"
#include "doc.hpp"
#include "slice.hpp"

namespace storage {

using encode_format = storage::impl::encode_format;
using trust_type = storage::impl::doc_t::trust_type;
using value_type = storage::impl::value_type;
using copy_flags = storage::impl::copy_flags;
using null_value_t = storage::impl::null_value_t;

using impl_value_t = const storage::impl::value_t*;
using impl_array_t = const storage::impl::array_t*;
using impl_dict_t = const storage::impl::dict_t*;
using impl_slot_t = storage::impl::value_slot_t*;
using impl_mutable_array_t = storage::impl::mutable_array_t*;
using impl_mutable_dict_t = storage::impl::mutable_dict_t*;
using impl_encoder_t = storage::impl::encoder_impl_t*;
using impl_shared_keys_t = storage::impl::shared_keys_t*;
using impl_key_path_t = storage::impl::path_t*;
using impl_deep_iterator_t = storage::impl::deep_iterator_t*;
using impl_doc_t = const storage::impl::doc_t*;

using time_stamp = int64_t;
constexpr time_stamp time_stamp_none = INT64_MIN;

extern const impl_value_t null_value;
extern const impl_array_t empty_array;
extern const impl_dict_t empty_dict;

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
    value_t(impl_value_t v);
    operator impl_value_t() const;

    static value_t null();

    value_type type() const;
    bool is_integer() const;
    bool is_unsigned() const;
    bool is_double() const;
    bool is_mutable() const;

    bool as_bool() const;
    int64_t as_int() const;
    uint64_t as_unsigned() const;
    float as_float() const;
    double as_double() const;
    slice_t as_string() const;
    time_stamp as_time_stamp() const;
    slice_t as_data() const;
    array_t as_array() const;
    dict_t as_dict() const;

    alloc_slice_t to_string() const;
    alloc_slice_t to_json(bool canonical = false) const;
    std::string to_json_string() const;
    std::string to_std_string() const;

    explicit operator bool() const;
    bool operator! () const;
    bool operator== (value_t v) const;
    bool operator== (impl_value_t v) const;
    bool operator!= (value_t v) const;
    bool operator!= (impl_value_t v) const;

    bool is_equals(value_t v) const;

    value_t& operator= (value_t v);
    value_t& operator= (std::nullptr_t);

    value_t operator[] (const key_path_t &kp) const;

    doc_t find_doc() const;

    static value_t from_data(slice_t data, trust_type trust = trust_type::untrusted);

    value_t(mutable_array_t&&) = delete;
    value_t& operator= (mutable_array_t&&) = delete;
    value_t(mutable_dict_t&&) = delete;
    value_t& operator= (mutable_dict_t&&) = delete;

protected:
    impl_value_t _val {nullptr};
};


class array_t : public value_t {
public:
    class iterator_t : private impl::array_iterator_t {
    public:
        iterator_t(array_t array);
        iterator_t(const array_iterator_t &i);
        value_t value() const;
        uint32_t count() const;
        bool next();
        impl_value_t operator -> () const;
        value_t operator * () const;
        explicit operator bool() const;
        iterator_t& operator++ ();
        bool operator!= (const iterator_t&);
        value_t operator[] (unsigned n) const;

    private:
        iterator_t() = default;

        friend class array_t;
    };


    array_t();
    array_t(impl_array_t a);
    operator impl_array_t () const;

    static array_t empty_array();

    uint32_t count() const;
    bool empty() const;
    value_t get(uint32_t index) const;

    value_t operator[] (int index) const;
    value_t operator[] (const key_path_t &kp) const;

    array_t& operator= (array_t a);
    array_t& operator= (std::nullptr_t);
    value_t& operator= (value_t v) = delete;

    inline mutable_array_t as_mutable() const;
    inline mutable_array_t mutable_copy(copy_flags = copy_flags::default_copy) const;

    iterator_t begin() const;
    iterator_t end() const;

    array_t(mutable_array_t&&) = delete;
    array_t& operator= (mutable_array_t&&) = delete;
};


class dict_t : public value_t {
public:
    class key_t {
    public:
        explicit key_t(nonnull_slice string);
        explicit key_t(alloc_slice_t string);
        const alloc_slice_t& string() const;
        operator const alloc_slice_t&() const;
        operator nonnull_slice() const;

    private:
        alloc_slice_t _str;
        impl::key_t _key;

        friend class dict_t;
    };


    class iterator_t : private impl::dict_iterator_t {
    public:
        iterator_t(dict_t dict);
        //iterator_t(const dict_iterator_t &i); //delete
        uint32_t count() const;
        value_t key() const;
        slice_t key_string() const;
        value_t value() const;
        bool next();
        impl_value_t operator -> () const;
        value_t operator * () const;
        explicit operator bool() const;
        iterator_t& operator++ ();
        bool operator!= (const iterator_t&) const;

    private:
        iterator_t() = default;

        friend class dict_t;
    };


    dict_t();
    dict_t(impl_dict_t d);
    operator impl_dict_t () const;

    static dict_t empty_dict();

    uint32_t count() const;
    bool empty() const;

    value_t get(nonnull_slice key) const;
    value_t get(const char* key NONNULL) const;

    value_t operator[] (nonnull_slice key) const;
    value_t operator[] (const char *key) const;
    value_t operator[] (const key_path_t &kp) const;

    dict_t& operator= (dict_t d);
    dict_t& operator= (std::nullptr_t);
    value_t& operator= (value_t v) = delete;

    inline mutable_dict_t as_mutable() const;
    inline mutable_dict_t mutable_copy(copy_flags = copy_flags::default_copy) const;

    value_t get(key_t &key) const;
    value_t operator[] (key_t &key) const;

    iterator_t begin() const;
    iterator_t end() const;

    dict_t(mutable_dict_t&&) = delete;
    dict_t& operator= (mutable_dict_t&&) = delete;
};


class key_path_t {
public:
    key_path_t(nonnull_slice spec, error_code *error);
    key_path_t(key_path_t &&kp);
    key_path_t& operator=(key_path_t &&kp);
    key_path_t(const key_path_t &kp);
    ~key_path_t();

    explicit operator bool() const;
    operator impl_key_path_t() const;
    explicit operator std::string() const;
    bool operator== (const key_path_t &kp) const;

    value_t eval(value_t root) const;
    static value_t eval(nonnull_slice specifier, value_t root, error_code *error);

private:
    key_path_t& operator=(const key_path_t&) = delete;

    impl_key_path_t _path;

    friend class value_t;
};


class deep_iterator_t {
public:
    deep_iterator_t(value_t v);
    ~deep_iterator_t();

    value_t value() const;
    slice_t key() const;
    uint32_t index() const;
    value_t parent() const;

    size_t depth() const;
    alloc_slice_t path_string() const;
    alloc_slice_t json_pointer() const;

    void skip_children();
    bool next();

    explicit operator bool() const;
    deep_iterator_t& operator++();

private:
    deep_iterator_t(const deep_iterator_t&) = delete;

    impl_deep_iterator_t _i;
};


class shared_keys_t {
public:
    shared_keys_t();
    shared_keys_t(impl_shared_keys_t sk);
    ~shared_keys_t();

    static shared_keys_t create();
    static shared_keys_t create(slice_t state);
    bool load_state(slice_t data);
    bool load_state(value_t state);
    alloc_slice_t state_data() const;
    void write_state(const encoder_t &enc);
    unsigned count() const;
    void revert_to_count(unsigned count);

    operator impl_shared_keys_t() const;
    bool operator== (shared_keys_t other) const;

    shared_keys_t(const shared_keys_t &other) noexcept;
    shared_keys_t(shared_keys_t &&other) noexcept;
    shared_keys_t& operator= (const shared_keys_t &other);
    shared_keys_t& operator= (shared_keys_t &&other) noexcept;

private:
    shared_keys_t(impl_shared_keys_t sk, int);
    impl_shared_keys_t _sk {nullptr};
};


class doc_t {
public:
    doc_t(alloc_slice_t data, trust_type trust = trust_type::untrusted,
          shared_keys_t sk = nullptr, slice_t extern_dest = null_slice) noexcept;

    static doc_t from_json(nonnull_slice json, error_code *out_error = nullptr);

    static alloc_slice_t dump(nonnull_slice data);

    doc_t();
    doc_t(impl_doc_t d, bool is_retain = true);
    doc_t(const doc_t &other) noexcept;
    doc_t(doc_t &&other) noexcept;
    doc_t& operator=(const doc_t &other);
    doc_t& operator=(doc_t &&other) noexcept;
    ~doc_t();

    slice_t data() const;
    alloc_slice_t alloced_data() const;
    shared_keys_t shared_keys() const;

    value_t root() const;
    explicit operator bool () const;
    array_t as_array() const;
    dict_t as_dict() const;

    operator value_t () const;
    operator dict_t () const;
    operator impl_dict_t () const;

    value_t operator[] (int index) const;
    value_t operator[] (slice_t key) const;
    value_t operator[] (const char *key) const;
    value_t operator[] (const key_path_t &kp) const;

    bool operator== (const doc_t &d) const;

    operator impl_doc_t() const;
    impl_doc_t detach();

    static doc_t containing(value_t v);
    bool set_associated(void *p, const char *t);
    void* associated(const char *type) const;

private:
    explicit doc_t(impl_value_t v);

    impl_doc_t _doc;

    friend class value_t;
};


class encoder_t {
public:
    class key_ref_t {
    public:
        key_ref_t(encoder_t &enc, slice_t key);

        template <class T>
        inline void operator= (T value) {
            _enc.write_key(_key);
            _enc << value;
        }
        void operator= (bool value);

    private:
        encoder_t &_enc;
        slice_t _key;
    };


    encoder_t();
    explicit encoder_t(encode_format format, size_t reserve_size = 0, bool unique_strings = true);
    explicit encoder_t(FILE *file, bool unique_strings = true);
    explicit encoder_t(shared_keys_t sk);
    explicit encoder_t(impl_encoder_t enc);
    encoder_t(encoder_t&& enc);

    ~encoder_t();

    void detach();
    void set_shared_keys(shared_keys_t sk);
    void amend(slice_t base, bool reuse_strings = false, bool extern_pointers = false);
    slice_t base() const;
    void suppress_trailer();

    operator impl_encoder_t () const;

    bool write_null();
    bool write_undefined();
    bool write_bool(bool value);
    bool write_int(int64_t i);
    bool write_uint(uint64_t i);
    bool write_float(float f);
    bool write_double(double d);
    bool write_string(slice_t value);
    bool write_string(const char *s);
    bool write_string(std::string s);
    bool write_date_string(time_stamp ts, bool utc = true);
    bool write_data(slice_t d);
    bool write_value(value_t value);
    bool convert_json(nonnull_slice json);
    bool begin_array(size_t reserve_count = 0);
    bool end_array();
    bool begin_dict(size_t reserve_count = 0);
    bool write_key(nonnull_slice key);
    bool write_key(value_t key);
    bool end_dict();

    template <class T>
    inline void write(nonnull_slice key, T value) {
        write_key(key);
        *this << value;
    }

    bool write_raw(slice_t data);

    size_t bytes_written_size() const;
    size_t next_write_pos() const;

    doc_t finish_doc(error_code *error = nullptr);
    alloc_slice_t finish(error_code *error = nullptr);
    size_t finish_item();
    void reset();

    error_code error() const;
    const char* error_message() const;

    encoder_t& operator<< (null_value_t);
    encoder_t& operator<< (long long i);
    encoder_t& operator<< (unsigned long long i);
    encoder_t& operator<< (long i);
    encoder_t& operator<< (unsigned long i);
    encoder_t& operator<< (int i);
    encoder_t& operator<< (unsigned int i);
    encoder_t& operator<< (double d);
    encoder_t& operator<< (float f);
    encoder_t& operator<< (slice_t s);
    encoder_t& operator<< (const  char *s);
    encoder_t& operator<< (const std::string &s);
    encoder_t& operator<< (value_t v);

    key_ref_t operator[] (nonnull_slice key);

    impl::encoder_t *get_encoder() const;
    impl::json_encoder_t *get_json_encoder() const;

    void record_exception(const std::exception &x);

protected:
    encoder_t(const encoder_t&) = delete;
    encoder_t& operator=(const encoder_t&) = delete;

    impl_encoder_t _enc;
};


class json_encoder_t : public encoder_t {
public:
    json_encoder_t();
    bool write_raw(slice_t raw);
};


class shared_encoder_t : public encoder_t {
public:
    explicit shared_encoder_t(impl_encoder_t enc);
    ~shared_encoder_t();
};


class json_delta_t {
public:
    static alloc_slice_t create(value_t old, value_t nuu);
    static bool create(value_t old, value_t nuu, encoder_t &encoder);
    static alloc_slice_t apply(value_t old, slice_t json_delta, error_code *error);
    static bool apply(value_t old, slice_t json_delta, encoder_t &encoder);
};


class alloced_dict_t : public dict_t, alloc_slice_t {
public:
    alloced_dict_t() = default;
    explicit alloced_dict_t(alloc_slice_t s);
    explicit alloced_dict_t(slice_t s);

    const alloc_slice_t& data() const;
    explicit operator bool () const;

    value_t operator[] (slice_t key) const;
    value_t operator[] (const char *key) const;
};

}
