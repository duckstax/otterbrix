#pragma once

#include <msgpack.hpp>
#include "document/support/ref_counted.hpp"
#include "document.hpp"

namespace document::impl {
class dict_t;
class array_t;
class value_t;
class dict_iterator_t;
}

namespace components::document {

using msgpack::type::object_type;
using msgpack::object;
using msgpack::object_handle;
using offset_t = std::size_t;
using version_t = uint16_t; //todo

enum class compare_t {
    less = -1,
    equals = 0,
    more = 1
};

class document_view_t final {
public:
    using index_t = const ::document::impl::dict_t*;
    using array_t = const ::document::impl::array_t*;
    using storage_t = const msgpack::sbuffer*;
    using iterator_t = ::document::impl::dict_iterator_t;

    document_view_t();
    document_view_t(index_t index, storage_t storage);
    explicit document_view_t(const document_t &src);
    explicit document_view_t(const document_ptr &src);
    document_view_t(const document_view_t &src);
    document_view_t &operator=(const document_view_t &src);

    bool is_valid() const;
    bool is_dict() const;
    bool is_array() const;
    std::size_t count() const;

    bool is_exists(std::string &&key) const;
    bool is_exists(const std::string &key) const;
    bool is_exists(uint32_t index) const;
    bool is_null(std::string &&key) const;
    bool is_null(const std::string &key) const;
    bool is_null(uint32_t index) const;
    bool is_bool(std::string &&key) const;
    bool is_bool(const std::string &key) const;
    bool is_bool(uint32_t index) const;
    bool is_ulong(std::string &&key) const;
    bool is_ulong(const std::string &key) const;
    bool is_ulong(uint32_t index) const;
    bool is_long(std::string &&key) const;
    bool is_long(const std::string &key) const;
    bool is_long(uint32_t index) const;
    bool is_float(std::string &&key) const;
    bool is_float(const std::string &key) const;
    bool is_float(uint32_t index) const;
    bool is_double(std::string &&key) const;
    bool is_double(const std::string &key) const;
    bool is_double(uint32_t index) const;
    bool is_string(std::string &&key) const;
    bool is_string(const std::string &key) const;
    bool is_string(uint32_t index) const;
    bool is_array(std::string &&key) const;
    bool is_array(const std::string &key) const;
    bool is_array(uint32_t index) const;
    bool is_dict(std::string &&key) const;
    bool is_dict(const std::string &key) const;
    bool is_dict(uint32_t index) const;

    object_handle get(std::string &&key) const;
    object_handle get(const std::string &key) const;
    object_handle get(uint32_t index) const;
    bool get_bool(std::string &&key) const;
    ulong get_ulong(std::string &&key) const;
    long get_long(std::string &&key) const;
    float get_float(std::string &&key) const;
    double get_double(std::string &&key) const;
    std::string get_string(std::string &&key) const;
    document_view_t get_array(std::string &&key) const;
    document_view_t get_array(const std::string &key) const;
    document_view_t get_array(uint32_t index) const;
    document_view_t get_dict(std::string &&key) const;
    document_view_t get_dict(const std::string &key) const;
    document_view_t get_dict(uint32_t index) const;

    template <class T>
    T get_as(std::string &&key) const {
        try {
            return get(std::move(key))->as<T>();
        } catch (...) {}
        return T();
    }

    template <class T>
    T get_as(const std::string &key) const {
        try {
            return get(key)->as<T>();
        } catch (...) {}
        return T();
    }

    template <class T>
    T get_as(uint32_t index) const {
        try {
            return get(index)->as<T>();
        } catch (...) {}
        return T();
    }

    iterator_t begin() const;

    compare_t compare(const document_view_t &other, const std::string &key) const;

    std::string to_json() const;
    ::document::retained_t<::document::impl::dict_t> to_dict() const;

private:
    index_t index_;
    array_t array_;
    storage_t storage_;

    document_view_t(array_t array, storage_t storage);

    object_type get_type(const ::document::impl::value_t *field) const;
    object_type get_type(std::string &&key) const;
    object_type get_type(const std::string &key) const;
    object_type get_type(uint32_t index) const;
    object_handle get_value(offset_t offset, std::size_t size) const;

    std::string to_json_dict() const;
    std::string to_json_array() const;
};

}
