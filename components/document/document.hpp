#pragma once

#include <memory>
#include <string>
#include <msgpack.hpp>
#include "support/ref_counted.hpp"
#include "document/mutable/mutable_array.h"

namespace storage::impl {
class mutable_dict_t;
class dict_t;
class dict_iterator_t;
}

namespace components::storage {

class document_t final {
    using storage_t = ::storage::impl::mutable_dict_t*;
    using const_storage_t = const ::storage::impl::dict_t*;
    using iterator = ::storage::impl::dict_iterator_t;

public:
    using value_t = const ::storage::impl::value_t*;

    document_t();
    explicit document_t(const ::storage::impl::dict_t *dict, bool is_owner = false);
    document_t(const document_t &src);
    ~document_t();

    void add_null(const std::string &key);
    void add_bool(const std::string &key, bool value);
    void add_ulong(const std::string &key, ulong value);
    void add_long(const std::string &key, long value);
    void add_double(const std::string &key, double value);
    void add_string(const std::string &key, std::string value);
    void add_array(const std::string &key, ::storage::impl::array_t *array);
    void add_dict(const std::string &key, ::storage::impl::dict_t *dict);
    void add_dict(const std::string &key, const document_t &dict);
    void add_dict(const std::string &key, document_t &&dict);

    bool is_exists(const std::string &key) const;
    bool is_null(const std::string &key) const;
    bool is_bool(const std::string &key) const;
    bool is_ulong(const std::string &key) const;
    bool is_long(const std::string &key) const;
    bool is_double(const std::string &key) const;
    bool is_string(const std::string &key) const;
    bool is_array(const std::string &key) const;
    bool is_dict(const std::string &key) const;

    const ::storage::impl::value_t *get(const std::string &key) const;
    bool get_bool(const std::string &key) const;
    ulong get_ulong(const std::string &key) const;
    long get_long(const std::string &key) const;
    double get_double(const std::string &key) const;
    std::string get_string(const std::string &key) const;
    const ::storage::impl::array_t *get_array(const std::string &key) const;
    document_t get_dict(const std::string &key) const;
    const_storage_t get_storage() const;

    template <class T> T get_as(const std::string &) const;

    ::storage::retained_const_t<::storage::impl::value_t> value() const;

    iterator begin() const;

    std::string to_json() const;
    static document_t from_json(const std::string &json);

    static ::storage::retained_t<::storage::impl::mutable_array_t> create_array();

    static msgpack::type::object_type get_msgpack_type(const ::storage::impl::value_t *value);
    static msgpack::object get_msgpack_object(const ::storage::impl::value_t *value);

private:
    storage_t storage_;
    bool is_owner_;
};


template <class T>
T document_t::get_as(const std::string &) const {
    static_assert(true, "not supported");
    return T();
}

template<> inline bool document_t::get_as<bool>(const std::string &key) const {
    return get_bool(key);
}

template<> inline ulong document_t::get_as<ulong>(const std::string &key) const {
    return get_ulong(key);
}

template<> inline long document_t::get_as<long>(const std::string &key) const {
    return get_long(key);
}

template<> inline double document_t::get_as<double>(const std::string &key) const {
    return get_double(key);
}

template<> inline std::string document_t::get_as<std::string>(const std::string &key) const {
    return get_string(key);
}

template<> inline const ::storage::impl::array_t *document_t::get_as<const ::storage::impl::array_t *>(const std::string &key) const {
    return get_array(key);
}

template<> inline document_t document_t::get_as<document_t>(const std::string &key) const {
    return get_dict(key);
}


using document_ptr = std::unique_ptr<document_t>;
auto make_document() -> document_ptr;

}
