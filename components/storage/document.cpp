#include "document.hpp"

namespace components::storage {

field_t::field_t(object_type type, version_t version, offset_t offset, offset_t size)
    : type(type)
    , version(version)
    , offset(offset)
    , size(size)
{}

field_t::field_t(object &&value, version_t version)
    : type(value.type)
    , value(std::move(value))
    , version(version)
    , offset(not_offset)
    , size(not_size)
{}

field_t::field_t(const object &value, version_t version)
    : type(value.type)
    , value(value)
    , version(version)
    , offset(not_offset)
    , size(not_size)
{}

bool field_t::is_value() const {
    return value.type == type;
}

bool field_t::is_nil() const {
    return type == object_type::NIL;
}

field_t field_t::nil() {
    static field_t nil_field(object_type::NIL);
    return nil_field;
}


document_t::document_t() {
}

document_t::document_t(const object &value) {
    add("", value);
}

void document_t::add(std::string &&key, const object &value) {
    fields_.emplace(std::move(key), field_t(value));
}

void document_t::add(std::string &&key, object &&value) {
    fields_.emplace(std::move(key), field_t(std::move(value)));
}

void document_t::add_null(std::string &&key) {
    fields_.emplace(std::move(key), field_t::nil());
}

void document_t::add_bool(std::string &&key, bool value) {
    add(std::move(key), object(value));
}

void document_t::add_ulong(std::string &&key, ulong value) {
    add(std::move(key), object(value));
}

void document_t::add_long(std::string &&key, long value) {
    add(std::move(key), object(value));
}

void document_t::add_float(std::string &&key, float value) {
    add(std::move(key), object(value));
}

void document_t::add_double(std::string &&key, double value) {
    add(std::move(key), object(value));
}

void document_t::add_string(std::string &&key, std::string value) {
    add(std::move(key), object(value.data()));
}

//void document_t::add_dict(std::string &&key, document_t &&dict) {
//    add(std::move(key), object(std::move(dict)));
//}

bool document_t::is_exists(std::string &&key) const {
    return fields_.find(std::move(key)) != fields_.cend();
}

bool document_t::is_null(std::string &&key) const {
    return get_type(std::move(key)) == object_type::NIL;
}

bool document_t::is_bool(std::string &&key) const {
    return get_type(std::move(key)) == object_type::BOOLEAN;
}

bool document_t::is_ulong(std::string &&key) const {
    return get_type(std::move(key)) == object_type::POSITIVE_INTEGER;
}

bool document_t::is_long(std::string &&key) const {
    return get_type(std::move(key)) == object_type::NEGATIVE_INTEGER;
}

bool document_t::is_float(std::string &&key) const {
    return get_type(std::move(key)) == object_type::FLOAT;
}

bool document_t::is_double(std::string &&key) const {
    return get_type(std::move(key)) == object_type::FLOAT64;
}

bool document_t::is_string(std::string &&key) const {
    return get_type(std::move(key)) == object_type::STR;
}

bool document_t::is_array(std::string &&key) const {
    return get_type(std::move(key)) == object_type::ARRAY;
}

bool document_t::is_dict(std::string &&key) const {
    return get_type(std::move(key)) == object_type::MAP;
}

const object &document_t::get(std::string &&key) const {
    //todo parse complex key
    auto field = fields_.find(std::move(key));
    if (field != fields_.cend()) {
        return get_value(field->second);
    } else {
        //todo error
    }
    static object nil;
    return nil;
}

const object &document_t::get(const std::string &key) const {
    //todo parse complex key
    auto field = fields_.find(key);
    if (field != fields_.cend()) {
        return get_value(field->second);
    } else {
        //todo error
    }
    static object nil;
    return nil;
}

bool document_t::get_bool(std::string &&key) const {
    return get_as<bool>(std::move(key));
}

ulong document_t::get_ulong(std::string &&key) const {
    return get_as<ulong>(std::move(key));
}

long document_t::get_long(std::string &&key) const {
    return get_as<long>(std::move(key));
}

float document_t::get_float(std::string &&key) const {
    return get_as<float>(std::move(key));
}

double document_t::get_double(std::string &&key) const {
    return get_as<double>(std::move(key));
}

std::string document_t::get_string(std::string &&key) const {
    return get_as<std::string>(std::move(key));
}

const document_t::fields_t &document_t::fields() const {
    return fields_;
}

document_t::const_iterator document_t::cbegin() const {
    return fields_.cbegin();
}

document_t::const_iterator document_t::cend() const {
    return fields_.cend();
}

std::string document_t::to_json() const {
    std::stringstream res;
    for (const auto& [key, value] : fields_) {
        if (!res.str().empty()) res << ",";
        res << key << ":" << get_value(value);
    }
    return "{" + res.str() + "}";
}

object_type document_t::get_type(const std::string &key) const {
    auto field = fields_.find(std::move(key));
    if (field != fields_.cend()) {
        return field->second.type;
    } else {
        //todo error
    }
    return object_type::NIL;
}

const object &document_t::get_value(const field_t &field) const {
    if (field.is_value()) return field.value;
    static object non_object;
    return non_object; //todo get object by offset
}


auto make_document() -> document_ptr {
    return std::make_unique<document_t>();
}

}
