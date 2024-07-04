#include "value.hpp"

namespace components::document {

    value_t::value_t(impl::element<impl::mutable_document> element)
        : element_(element) {}

    void value_t::set(impl::element<impl::mutable_document> element) { element_ = element; }

    types::physical_type value_t::physical_type() const noexcept { return element_.physical_type(); }

    types::logical_type value_t::logical_type() const noexcept { return element_.logical_type(); }

    bool value_t::operator==(const value_t& rhs) const {
        switch (physical_type()) {
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return as_unsigned() == rhs.as_unsigned();
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return as_int() == rhs.as_int();
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return as_int128() == rhs.as_int128();
            case types::physical_type::FLOAT:
                return is_equals(as_float(), rhs.as_float());
            case types::physical_type::DOUBLE:
                return is_equals(as_double(), rhs.as_double());
            case types::physical_type::STRING:
                return as_string() == rhs.as_string();
            default: // special values are always equal
                return physical_type() == rhs.physical_type();
        }
    }

    bool value_t::operator<(const value_t& rhs) const {
        switch (physical_type()) {
            case types::physical_type::BOOL_FALSE:
            case types::physical_type::BOOL_TRUE:
                return as_bool() < rhs.as_bool();
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return as_unsigned() < rhs.as_unsigned();
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return as_int() < rhs.as_int();
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return as_int128() < rhs.as_int128();
            case types::physical_type::FLOAT:
                return as_float() < rhs.as_float();
            case types::physical_type::DOUBLE:
                return as_double() < rhs.as_double();
            case types::physical_type::STRING:
                return as_string() < rhs.as_string();
            default: // special values are always equal
                return false;
        }
    }

    bool value_t::as_bool() const noexcept { return element_.get_bool().value(); }

    int64_t value_t::as_int() const noexcept { return element_.get_int64().value(); }

    uint64_t value_t::as_unsigned() const noexcept { return element_.get_uint64().value(); }

    int128_t value_t::as_int128() const noexcept { return element_.get_int128().value(); }

    float value_t::as_float() const noexcept { return element_.get_float().value(); }

    double value_t::as_double() const noexcept { return element_.get_double().value(); }

    std::string_view value_t::as_string() const noexcept { return element_.get_string().value(); }

    bool value_t::is_bool() const noexcept {
        switch (physical_type()) {
            case types::physical_type::BOOL_TRUE:
            case types::physical_type::BOOL_FALSE:
                return true;
            default:
                return false;
        }
    }

    bool value_t::is_int() const noexcept {
        switch (physical_type()) {
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
            case types::physical_type::INT128:
                return true;
            default:
                return false;
        }
    }

    bool value_t::is_unsigned() const noexcept {
        switch (physical_type()) {
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
            case types::physical_type::UINT128:
                return true;
            default:
                return false;
        }
    }

    bool value_t::is_int128() const noexcept {
        return physical_type() == types::physical_type::UINT128 || physical_type() == types::physical_type::INT128;
    }

    bool value_t::is_double() const noexcept {
        return physical_type() == types::physical_type::FLOAT || physical_type() == types::physical_type::DOUBLE;
    }

    bool value_t::is_string() const noexcept { return physical_type() == types::physical_type::STRING; }

    value_t::operator bool() const { return element_.usable(); }

    const impl::element<impl::mutable_document>* value_t::element() const noexcept { return &element_; }

    value_t sum(const value_t& value1,
                const value_t& value2,
                impl::mutable_document* tape,
                std::pmr::memory_resource* resource) {
        if (!value1) {
            return value2;
        }
        if (!value2) {
            return value1;
        }

        switch (value1.physical_type()) {
            case types::physical_type::BOOL_FALSE:
            case types::physical_type::BOOL_TRUE:
                return value_t{resource, tape, value1.as_bool() + value2.as_bool()};
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return value_t{resource, tape, value1.as_unsigned() + value2.as_unsigned()};
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return value_t{resource, tape, value1.as_int() + value2.as_int()};
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return value_t{resource, tape, value1.as_int128() + value2.as_int128()};
            case types::physical_type::FLOAT:
                return value_t{resource, tape, value1.as_float() + value2.as_float()};
            case types::physical_type::DOUBLE:
                return value_t{resource, tape, value1.as_double() + value2.as_double()};
            case types::physical_type::STRING:
                return value_t{resource,
                               tape,
                               std::pmr::string(value1.as_string(), resource) +
                                   std::pmr::string(value2.as_string(), resource)};
            default: // special values can't be addad
                return value1;
        }
    }
} // namespace components::document