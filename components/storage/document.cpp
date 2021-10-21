#include <components/storage/document.hpp>
#include <msgpack.hpp>
#include "storage/mutable/mutable_dict.h"

using ::storage::impl::mutable_dict_t;

namespace components::storage {

field_t::field_t(version_t version, value_type type, offset_t offset, offset_t size)
    : version(version)
    , type(type)
    , offset(offset)
    , size(size)
    , null_(false)
{}

bool field_t::is_null() const {
    return null_;
}

field_t field_t::null() {
    static field_t null_field(0, value_type::undefined);
    null_field.null_ = true;
    return null_field;
}


st_document_t::st_document_t(std::stringstream *data)
    : index_(mutable_dict_t::new_dict())
    , data_(data ? data : new std::stringstream)
    , is_owner_data_(data == nullptr)
{}

st_document_t::st_document_t(st_document_t *parent)
    : index_(mutable_dict_t::new_dict())
    , data_(parent ? parent->data_ : new std::stringstream)
    , is_owner_data_(parent == nullptr)
{}

st_document_t::~st_document_t() {
    if (is_owner_data_) delete data_;
}

void st_document_t::add_null(std::string &&key) {
    add_index_(std::move(key), field_t(default_version, value_type::null));
}

void st_document_t::add_bool(std::string &&key, bool value) {
    add_value_(std::move(key), value, value_type::boolean);
}

void st_document_t::add_long(std::string &&key, long value) {
    add_value_(std::move(key), value, value_type::long_number);
}

void st_document_t::add_double(std::string &&key, double value) {
    add_value_(std::move(key), value, value_type::double_number);
}

void st_document_t::add_string(std::string &&key, std::string &&value) {
    add_value_(std::move(key), std::move(value), value_type::string);
}

void st_document_t::add_dict(std::string &&key, st_document_t &&dict) {
    auto offset = data_->str().size();
    msgpack::pack(*data_, reinterpret_cast<uint64_t>(dict.data_));
    dict.is_owner_data_ = false;
    auto size = data_->str().size() - offset;
    add_index_(std::move(key), field_t(default_version, value_type::dict, offset, size));
}

bool st_document_t::is_exists(std::string &&key) const {
    return index_->get(std::move(key)) != nullptr;
}

bool st_document_t::is_null(std::string &&key) const {
    return is_type_(std::move(key), value_type::null);
}

bool st_document_t::is_bool(std::string &&key) const {
    return is_type_(std::move(key), value_type::boolean);
}

bool st_document_t::is_long(std::string &&key) const {
    return is_type_(std::move(key), value_type::long_number);
}

bool st_document_t::is_double(std::string &&key) const {
    return is_type_(std::move(key), value_type::double_number);
}

bool st_document_t::is_string(std::string &&key) const {
    return is_type_(std::move(key), value_type::string);
}

bool st_document_t::is_dict(std::string &&key) const {
    return is_type_(std::move(key), value_type::dict);
}

bool st_document_t::is_array(std::string &&key) const {
    return is_type_(std::move(key), value_type::array);
}

bool st_document_t::as_bool(std::string &&key) const {
    auto index = get_index_(std::move(key));
    if (!index.is_null() && index.type == value_type::boolean) {
        return get_value_<bool>(index.offset, index.size);
    }
    return false;
}

long st_document_t::as_long(std::string &&key) const {
    auto index = get_index_(std::move(key));
    if (!index.is_null() && index.type == value_type::long_number) {
        return get_value_<long>(index.offset, index.size);
    }
    return 0;
}

double st_document_t::as_double(std::string &&key) const {
    auto index = get_index_(std::move(key));
    if (!index.is_null() && index.type == value_type::double_number) {
        return get_value_<double>(index.offset, index.size);
    }
    return .0;
}

std::string st_document_t::as_string(std::string &&key) const {
    auto index = get_index_(std::move(key));
    if (!index.is_null() && index.type == value_type::string) {
        return get_value_<std::string>(index.offset, index.size);
    }
    return "";
}

st_document_t st_document_t::as_dict(std::string &&key) const {
    auto index = get_index_(std::move(key));
    if (!index.is_null() && index.type == value_type::dict) {
        auto data = reinterpret_cast<std::stringstream*>(get_value_<uint64_t>(index.offset, index.size));
        st_document_t doc(data);
        //doc.index_ = ::storage::retained_t(index.);
        return doc;
    }
    return st_document_t();
}

std::string st_document_t::to_json_index() const {
    return index_->to_json_string();
}

void st_document_t::add_index_(std::string &&key, const field_t &field) {
    auto index = mutable_dict_t::new_dict();
    index->set(key_version, field.version); //todo not write default version
    index->set(key_type, static_cast<int>(field.type));
    if (field.offset != not_offset) index->set(key_offset, field.offset);
    if (field.size != not_offset) index->set(key_size, field.size);
    index_->set(std::move(key), index);
}

template <class T>
void st_document_t::add_value_(std::string &&key, T value, value_type type) {
    auto offset = data_->str().size();
    msgpack::pack(*data_, value);
    auto size = data_->str().size() - offset;
    add_index_(std::move(key), field_t(default_version, type, offset, size));
}

bool st_document_t::is_type_(std::string &&key, value_type type) const {
    auto index = index_->get(std::move(key));
    if (index) {
        auto type_val = index->as_dict()->get(key_type);
        return type_val && static_cast<value_type>(type_val->as_int()) == type;
    }
    return false;
}

field_t st_document_t::get_index_(std::string &&key) const {
    auto index = index_->get(std::move(key));
    if (index) {
        auto version = index->as_dict()->get(key_version);
        auto type = index->as_dict()->get(key_type);
        auto offset = index->as_dict()->get(key_offset);
        auto size = index->as_dict()->get(key_size);
        return field_t(version ? static_cast<version_t>(version->as_int()) : 0,
                       type ? static_cast<value_type>(type->as_int()) : value_type::undefined,
                       offset ? static_cast<offset_t>(offset->as_int()) : 0,
                       size ? static_cast<offset_t>(size->as_int()) : 0);
    }
    return field_t::null();
}

template <class T>
T st_document_t::get_value_(offset_t offset, offset_t size) const {
    assert(offset + size > data_->str().size());
    auto data = data_->str().substr(offset, size);
    msgpack::object_handle oh = msgpack::unpack(data.data(), data.size());
    msgpack::object object = oh.get();
    T t;
    object.convert(t);
    return t;
}




//////////////////////////// OLD ////////////////////////////

    auto make_document() -> document_ptr {
        return std::make_unique<document_t>();

    }

    void document_t::add(std::string &&key) {
        json_.emplace(std::move(key), nullptr);
    }

    void document_t::add(std::string &&key, bool value) {
        json_.emplace(std::move(key), value);
    }

    void document_t::add(std::string &&key, long value) {
        json_.emplace(std::move(key), value);
    }

    void document_t::add(std::string &&key, double value) {
        json_.emplace(std::move(key), value);
    }

    void document_t::add(std::string &&key, const std::string &value) {
        json_.emplace(std::move(key), value);
    }

    auto document_t::to_string() const -> std::string {
        return json_.dump();
    }

    auto document_t::get(const std::string &name) -> document_t {
        return document_t( json_.at(name));
    }

    bool document_t::as_bool() const {
        return json_.get<bool>();
    }

    std::string document_t::as_string() const {
        return json_.get<std::string>();
    }

    double document_t::as_double() const {
        return json_.get<double>();
    }

    int32_t document_t::as_int32() const {
        return json_.get<int32_t>();
    }

    document_t::json_t::array_t document_t::as_array() const {
        return json_.get<json_t::array_t>();
    }

    bool document_t::is_boolean() const noexcept {
        return json_.is_boolean();
    }

    bool document_t::is_integer() const noexcept {
        return json_.is_number_integer();
    }

    bool document_t::is_float() const noexcept {
        return json_.is_number_float();
    }

    bool document_t::is_string() const noexcept {
        return json_.is_string();
    }

    bool document_t::is_null() const noexcept {
        return json_.is_null();
    }

    bool document_t::is_object() const noexcept {
        return json_.is_object();
    }

    bool document_t::is_array() const noexcept {
        return json_.is_array();
    }

    document_t::document_t() {
        json_ = json_t::object();
    }

    document_t::document_t(document_t::json_t value) : json_(std::move(value)) {}

}
