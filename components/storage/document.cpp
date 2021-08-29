#include <components/storage/document.hpp>

namespace components::storage {

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