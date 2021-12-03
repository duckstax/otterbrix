#include "document.hpp"
#include "mutable/mutable_dict.h"
#include "document/json/json_coder.hpp"
#include <iostream>

using ::document::impl::mutable_array_t;
using ::document::impl::mutable_dict_t;
using ::document::impl::value_t;
using ::document::impl::value_type;

namespace components::document {

document_t::document_t()
    : storage_(mutable_dict_t::new_dict().detach())
    , is_owner_(true) {
}

document_t::document_t(const ::document::impl::dict_t *dict, bool is_owner)
    : storage_(dict->as_mutable())
    , is_owner_(is_owner) {
}

document_t::document_t(const document_t &src)
    : storage_(src.storage_)
    , is_owner_(false) {
}

document_t::document_t(document_t &&src)
    : storage_(src.storage_)
    , is_owner_(src.is_owner_) {
    src.is_owner_ = false;
    src.storage_ = nullptr;
}

document_t::~document_t() {
    if (is_owner_ && storage_) storage_->_release();
}

void document_t::add_null(const std::string &key) {
    storage_->set(key, ::document::impl::null_value);
}

void document_t::add_bool(const std::string &key, bool value) {
    storage_->set(key, value);
}

void document_t::add_ulong(const std::string &key, ulong value) {
    storage_->set(key, value);
}

void document_t::add_long(const std::string &key, long value) {
    storage_->set(key, value);
}

void document_t::add_double(const std::string &key, double value) {
    storage_->set(key, value);
}

void document_t::add_string(const std::string &key, std::string value) {
    storage_->set(key, value);
}

void document_t::add_array(const std::string &key, ::document::impl::array_t *array) {
    storage_->set(key, array);
}

void document_t::add_dict(const std::string &key, ::document::impl::dict_t *dict) {
    storage_->set(key, dict);
}

void document_t::add_dict(const std::string &key, const document_t &dict) {
    storage_->set(key, dict.storage_);
}

void document_t::add_dict(const std::string &key, document_t &&dict) {
    storage_->set(key, dict.storage_);
    //    dict.storage_ = nullptr;
}

void document_t::add(const std::string &key, const ::document::impl::value_t *value) {
    storage_->set(key, value);
}

bool document_t::is_exists(const std::string &key) const {
    return storage_->get(key) != nullptr;
}

bool document_t::is_null(const std::string &key) const {
    return storage_->get(key)->type() == value_type::null;
}

bool document_t::is_bool(const std::string &key) const {
    return storage_->get(key)->type() == value_type::boolean;
}

bool document_t::is_ulong(const std::string &key) const {
    return storage_->get(key)->is_unsigned();
}

bool document_t::is_long(const std::string &key) const {
    return storage_->get(key)->is_int();
}

bool document_t::is_double(const std::string &key) const {
    return storage_->get(key)->is_double();
}

bool document_t::is_string(const std::string &key) const {
    return storage_->get(key)->type() == value_type::string;
}

bool document_t::is_array(const std::string &key) const {
    return storage_->get(key)->type() == value_type::array;
}

bool document_t::is_dict(const std::string &key) const {
    return storage_->get(key)->type() == value_type::dict;
}

const value_t *document_t::get(const std::string &key) const {
    return storage_->get(key);
}

bool document_t::get_bool(const std::string &key) const {
    return get(key)->as_bool();
}

ulong document_t::get_ulong(const std::string &key) const {
    return get(key)->as_unsigned();
}

long document_t::get_long(const std::string &key) const {
    return get(key)->as_int();
}

double document_t::get_double(const std::string &key) const {
    return get(key)->as_double();
}

std::string document_t::get_string(const std::string &key) const {
    return static_cast<std::string>(get(key)->as_string());
}

const ::document::impl::array_t *document_t::get_array(const std::string &key) const {
    return get(key)->as_array();
}

document_t document_t::get_dict(const std::string &key) const {
    return document_t(get(key)->as_dict());
}

document_t::const_storage_t document_t::get_storage() const {
    return storage_->as_dict();
}

::document::retained_const_t<::document::impl::value_t> document_t::value() const {
    return storage_->as_dict();
}

document_t::iterator document_t::begin() const {
    return storage_->begin();
}

std::string document_t::to_json() const {
    return storage_->to_json_string();
}

document_t document_t::from_json(const std::string &json) {
    auto doc = ::document::impl::doc_t::from_json(json);
    auto dict = mutable_dict_t::new_dict(doc->root()->as_dict()).detach();
    document_t document(dict, true);
    return document;
}

::document::retained_t<::document::impl::mutable_array_t> document_t::create_array() {
    return mutable_array_t::new_array();
}

msgpack::type::object_type document_t::get_msgpack_type(const ::document::impl::value_t *value) {
    if (value->type() == value_type::null) return msgpack::type::NIL;
    if (value->type() == value_type::boolean) return msgpack::type::BOOLEAN;
    if (value->is_unsigned()) return msgpack::type::POSITIVE_INTEGER;
    if (value->is_int()) return msgpack::type::NEGATIVE_INTEGER;
    if (value->is_double()) return msgpack::type::FLOAT64;
    if (value->type() == value_type::string) return msgpack::type::STR;
    if (value->type() == value_type::array) return msgpack::type::ARRAY;
    if (value->type() == value_type::dict) return msgpack::type::MAP;
    return msgpack::type::NIL;
}

msgpack::object document_t::get_msgpack_object(const ::document::impl::value_t *value) {
    if (value->type() == value_type::boolean) return msgpack::object(value->as_bool());
    if (value->is_unsigned()) return msgpack::object(value->as_unsigned());
    if (value->is_int()) return msgpack::object(value->as_int());
    if (value->is_double()) return msgpack::object(value->as_double());
    if (value->type() == value_type::string) return msgpack::object(std::string_view(value->as_string()));
    return msgpack::object();
}

auto make_document() -> document_ptr {
    return std::make_unique<document_t>();
}

}
