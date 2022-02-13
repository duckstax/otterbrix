#include "document_id.hpp"
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace components::document {

    document_id_t::document_id_t(std::string id)
        : value_(std::move(id)) {}

    document_id_t::document_id_t(const std::string_view& id)
        : value_(id) {}

    document_id_t::document_id_t(document_id_t&& id) noexcept
        : value_(std::move(id.value_))
        , null_(id.null_) {}

    document_id_t& document_id_t::operator=(std::string id) {
        value_ = std::move(id);
        return *this;
    }

    document_id_t& document_id_t::operator=(const std::string_view& id) {
        value_ = id;
        return *this;
    }

    document_id_t& document_id_t::operator=(const document_id_t& id) {
        value_ = id.value_;
        null_ = id.null_;
        return *this;
    }

    document_id_t& document_id_t::operator=(document_id_t&& id) {
        value_ = std::move(id.value_);
        null_ = id.null_;
        id.null_ = true;
        return *this;
    }

    document_id_t document_id_t::generate() {
        static boost::uuids::random_generator generator;
        return document_id_t(boost::uuids::to_string(generator()));
    }

    document_id_t document_id_t::null_id() {
        return document_id_t();
    }

    bool document_id_t::operator==(const document_id_t& other) const {
        return value_ == other.value_;
    }

    bool document_id_t::operator!=(const document_id_t& other) const {
        return value_ != other.value_;
    }

    bool document_id_t::operator<(const document_id_t& other) const {
        return value_ < other.value_;
    }

    std::string document_id_t::to_string() const {
        return value_;
    }

    std::string_view document_id_t::to_string_view() const {
        return std::string_view(value_);
    }

    bool document_id_t::is_null() const {
        return null_;
    }

    document_id_t::document_id_t()
        : null_(true) {}

}