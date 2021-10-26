#pragma once

#include <memory>
#include <msgpack.hpp>

namespace components::storage {

using msgpack::type::object_type;
using msgpack::object;
using offset_t = std::size_t;
using version_t = uint16_t; //todo

constexpr version_t default_version  = 0;
constexpr offset_t not_offset        = 0;
constexpr offset_t not_size          = 0;
constexpr const char *key_version = "$v";
constexpr const char *key_offset  = "$o";
constexpr const char *key_size    = "$s";

struct field_t {
    object_type type;
    object value;
    version_t version;
    offset_t offset;
    offset_t size;

    field_t(object_type type, version_t version = default_version, offset_t offset = not_offset, offset_t size = not_offset);
    field_t(object &&value, version_t version = default_version);
    field_t(const object &value, version_t version = default_version);

    bool is_value() const;
    bool is_nil() const;
    static field_t nil();
};


class document_t final {
    using fields_t = std::map<std::string, field_t>;
    using const_iterator = fields_t::const_iterator;

public:
    document_t();
    explicit document_t(const object &value);

    void add(std::string &&key, const object &value);
    void add(std::string &&key, object &&value);
    void add_null(std::string &&key);
    void add_bool(std::string &&key, bool value);
    void add_ulong(std::string &&key, ulong value);
    void add_long(std::string &&key, long value);
    void add_float(std::string &&key, float value);
    void add_double(std::string &&key, double value);
    void add_string(std::string &&key, std::string value);
//    void add_array(std::string &&key);
//    void add_dict(std::string &&key, document_t &&dict);

    bool is_exists(std::string &&key) const;
    bool is_null(std::string &&key) const;
    bool is_bool(std::string &&key) const;
    bool is_ulong(std::string &&key) const;
    bool is_long(std::string &&key) const;
    bool is_float(std::string &&key) const;
    bool is_double(std::string &&key) const;
    bool is_string(std::string &&key) const;
    bool is_array(std::string &&key) const;
    bool is_dict(std::string &&key) const;

    const object &get(std::string &&key) const;
    const object &get(const std::string &key) const;
    bool get_bool(std::string &&key) const;
    ulong get_ulong(std::string &&key) const;
    long get_long(std::string &&key) const;
    float get_float(std::string &&key) const;
    double get_double(std::string &&key) const;
    std::string get_string(std::string &&key) const;
//    void as_array(std::string &&key) const; //todo
//    document_t get_dict(std::string &&key) const;

    template <class T>
    T get_as(const std::string &key) const {
        try {
            return get(key).as<T>();
        } catch (...) {
            return T();
        }
    }

    const fields_t& fields() const;
    const_iterator cbegin() const;
    const_iterator cend() const;

    std::string to_json() const;

private:
    fields_t fields_;

    object_type get_type(const std::string &key) const;
    const object &get_value(const field_t &field) const;
};

using document_ptr = std::unique_ptr<document_t>;
auto make_document() -> document_ptr;

}
