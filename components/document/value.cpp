#include "value.hpp"

namespace components::new_document {

    void value_t::set(impl::element<impl::mutable_document> element) {
        element_ = element;
    }

    types::physical_type value_t::physical_type() const noexcept { return element_.physical_type(); }

    types::logical_type value_t::logical_type() const noexcept { return element_.logical_type(); }

    static inline bool is_equals(double x, double y) {
        return std::fabs(x - y) < std::numeric_limits<double>::epsilon();
    }

    bool value_t::operator==(const value_t& rhs) const {
        switch (physical_type()) {
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return element_.get_uint64().value() == rhs.element_.get_uint64().value();
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return element_.get_int64().value() == rhs.element_.get_int64().value();
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return element_.get_int128().value() == rhs.element_.get_int128().value();
            case types::physical_type::FLOAT:
            case types::physical_type::DOUBLE:
                return is_equals(element_.get_double().value(), rhs.element_.get_double().value());
            case types::physical_type::STRING:
                return element_.get_string().value() == rhs.element_.get_string().value();
            default: // special values are always equal
                return physical_type() == rhs.physical_type();
        }
    }

    bool value_t::operator<(const value_t& rhs) const {
        switch (physical_type()) {
            case types::physical_type::BOOL_FALSE:
            case types::physical_type::BOOL_TRUE:
                return element_.get_bool().value() < rhs.element_.get_bool().value();
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return element_.get_uint64().value() < rhs.element_.get_uint64().value();
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return element_.get_int64().value() < rhs.element_.get_int64().value();
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return element_.get_int128().value() < rhs.element_.get_int128().value();
            case types::physical_type::FLOAT:
            case types::physical_type::DOUBLE:
                return element_.get_double().value() < rhs.element_.get_double().value();
            case types::physical_type::STRING:
                return element_.get_string().value() < rhs.element_.get_string().value();
            default: // special values are always equal
                return false;
        }
    }

    bool value_t::as_bool() const noexcept { return element_.get_bool().value(); }

    int64_t value_t::as_int() const noexcept { return element_.get_int64().value(); }

    uint64_t value_t::as_unsigned() const noexcept { return element_.get_uint64().value(); }

    float value_t::as_float() const noexcept { return element_.get_float().value(); }

    double value_t::as_double() const noexcept { return element_.get_double().value(); }

    std::string_view value_t::as_string() const noexcept { return element_.get_string().value(); }

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

    bool value_t::is_double() const noexcept {
        return physical_type() == types::physical_type::FLOAT || physical_type() == types::physical_type::DOUBLE;
    }

    bool value_t::is_string() const noexcept { return physical_type() == types::physical_type::STRING; }
} // namespace components::new_document