#include "logical_value.hpp"

namespace components::types {

    logical_value_t::logical_value_t(complex_logical_type type)
        : type_(std::move(type)) {
        switch (type_.type()) {
            case logical_type::NA:
            case logical_type::POINTER:
                value_ = nullptr;
            case logical_type::BOOLEAN:
                value_ = false;
                break;
            case logical_type::TINYINT:
                value_ = int8_t{0};
                break;
            case logical_type::SMALLINT:
                value_ = int16_t{0};
                break;
            case logical_type::INTEGER:
                value_ = int32_t{0};
                break;
            case logical_type::BIGINT:
                value_ = int64_t{0};
                break;
            case logical_type::HUGEINT:
                break;
            case logical_type::TIMESTAMP_SEC:
                value_ = std::chrono::seconds{0};
                break;
            case logical_type::TIMESTAMP_MS:
                value_ = std::chrono::milliseconds{0};
                break;
            case logical_type::TIMESTAMP_US:
                value_ = std::chrono::microseconds{0};
                break;
            case logical_type::TIMESTAMP_NS:
                value_ = std::chrono::nanoseconds{0};
                break;
            case logical_type::FLOAT:
                value_ = float{0};
                break;
            case logical_type::DOUBLE:
                value_ = double{0};
                break;
            case logical_type::UTINYINT:
                value_ = uint8_t{0};
                break;
            case logical_type::USMALLINT:
                value_ = uint16_t{0};
                break;
            case logical_type::UINTEGER:
                value_ = uint32_t{0};
                break;
            case logical_type::UBIGINT:
                value_ = uint64_t{0};
                break;
            case logical_type::INVALID:
                assert(false && "cannot create value of invalid type");
        }
    }

    logical_value_t::logical_value_t(const logical_value_t& other)
        : type_(other.type_) {
        switch (type_.type()) {
            case logical_type::BOOLEAN:
                value_ = std::get<bool>(other.value_);
                break;
            case logical_type::TINYINT:
                value_ = std::get<int8_t>(other.value_);
                break;
            case logical_type::SMALLINT:
                value_ = std::get<int16_t>(other.value_);
                break;
            case logical_type::INTEGER:
                value_ = std::get<int32_t>(other.value_);
                break;
            case logical_type::BIGINT:
                value_ = std::get<int64_t>(other.value_);
                break;
            case logical_type::FLOAT:
                value_ = std::get<float>(other.value_);
                break;
            case logical_type::DOUBLE:
                value_ = std::get<double>(other.value_);
                break;
            case logical_type::UTINYINT:
                value_ = std::get<uint8_t>(other.value_);
                break;
            case logical_type::USMALLINT:
                value_ = std::get<uint16_t>(other.value_);
                break;
            case logical_type::UINTEGER:
                value_ = std::get<uint32_t>(other.value_);
                break;
            case logical_type::UBIGINT:
                value_ = std::get<uint64_t>(other.value_);
                break;
            case logical_type::TIMESTAMP_NS:
                value_ = std::get<std::chrono::nanoseconds>(other.value_);
                break;
            case logical_type::TIMESTAMP_US:
                value_ = std::get<std::chrono::microseconds>(other.value_);
                break;
            case logical_type::TIMESTAMP_MS:
                value_ = std::get<std::chrono::milliseconds>(other.value_);
                break;
            case logical_type::TIMESTAMP_SEC:
                value_ = std::get<std::chrono::seconds>(other.value_);
                break;
            case logical_type::STRING_LITERAL:
                value_ = std::make_unique<std::string>(*std::get<std::unique_ptr<std::string>>(other.value_));
                break;
            case logical_type::POINTER:
                value_ = std::get<void*>(other.value_);
                break;
            case logical_type::LIST:
            case logical_type::ARRAY:
            case logical_type::MAP:
            case logical_type::STRUCT:
                value_ = std::make_unique<std::vector<logical_value_t>>(
                    *std::get<std::unique_ptr<std::vector<logical_value_t>>>(other.value_));
                break;
            default:
                value_ = nullptr;
        }
    }

    logical_value_t::logical_value_t(logical_value_t&& other) noexcept
        : type_(std::move(other.type_)) {
        switch (type_.type()) {
            case logical_type::BOOLEAN:
                value_ = std::get<bool>(other.value_);
                break;
            case logical_type::TINYINT:
                value_ = std::get<int8_t>(other.value_);
                break;
            case logical_type::SMALLINT:
                value_ = std::get<int16_t>(other.value_);
                break;
            case logical_type::INTEGER:
                value_ = std::get<int32_t>(other.value_);
                break;
            case logical_type::BIGINT:
                value_ = std::get<int64_t>(other.value_);
                break;
            case logical_type::FLOAT:
                value_ = std::get<float>(other.value_);
                break;
            case logical_type::DOUBLE:
                value_ = std::get<double>(other.value_);
                break;
            case logical_type::UTINYINT:
                value_ = std::get<uint8_t>(other.value_);
                break;
            case logical_type::USMALLINT:
                value_ = std::get<uint16_t>(other.value_);
                break;
            case logical_type::UINTEGER:
                value_ = std::get<uint32_t>(other.value_);
                break;
            case logical_type::UBIGINT:
                value_ = std::get<uint64_t>(other.value_);
                break;
            case logical_type::STRING_LITERAL:
                value_ = std::move(std::get<std::unique_ptr<std::string>>(other.value_));
                break;
            case logical_type::POINTER:
                value_ = std::get<void*>(other.value_);
                break;
            case logical_type::LIST:
            case logical_type::ARRAY:
            case logical_type::MAP:
            case logical_type::STRUCT:
                value_ = std::move(std::get<std::unique_ptr<std::vector<logical_value_t>>>(other.value_));
                break;
            default:
                value_ = nullptr;
        }
    }

    logical_value_t& logical_value_t::operator=(const logical_value_t& other) {
        type_ = other.type_;
        switch (type_.type()) {
            case logical_type::BOOLEAN:
                value_ = std::get<bool>(other.value_);
                break;
            case logical_type::TINYINT:
                value_ = std::get<int8_t>(other.value_);
                break;
            case logical_type::SMALLINT:
                value_ = std::get<int16_t>(other.value_);
                break;
            case logical_type::INTEGER:
                value_ = std::get<int32_t>(other.value_);
                break;
            case logical_type::BIGINT:
                value_ = std::get<int64_t>(other.value_);
                break;
            case logical_type::FLOAT:
                value_ = std::get<float>(other.value_);
                break;
            case logical_type::DOUBLE:
                value_ = std::get<double>(other.value_);
                break;
            case logical_type::UTINYINT:
                value_ = std::get<uint8_t>(other.value_);
                break;
            case logical_type::USMALLINT:
                value_ = std::get<uint16_t>(other.value_);
                break;
            case logical_type::UINTEGER:
                value_ = std::get<uint32_t>(other.value_);
                break;
            case logical_type::UBIGINT:
                value_ = std::get<uint64_t>(other.value_);
                break;
            case logical_type::STRING_LITERAL:
                value_ = std::make_unique<std::string>(*std::get<std::unique_ptr<std::string>>(other.value_));
                break;
            case logical_type::POINTER:
                value_ = std::get<void*>(other.value_);
                break;
            case logical_type::LIST:
            case logical_type::ARRAY:
            case logical_type::MAP:
            case logical_type::STRUCT:
                value_ = std::make_unique<std::vector<logical_value_t>>(
                    *std::get<std::unique_ptr<std::vector<logical_value_t>>>(other.value_));
                break;
            default:
                value_ = nullptr;
        }
        return *this;
    }

    logical_value_t& logical_value_t::operator=(logical_value_t&& other) noexcept {
        type_ = std::move(other.type_);
        switch (type_.type()) {
            case logical_type::BOOLEAN:
                value_ = std::get<bool>(other.value_);
                break;
            case logical_type::TINYINT:
                value_ = std::get<int8_t>(other.value_);
                break;
            case logical_type::SMALLINT:
                value_ = std::get<int16_t>(other.value_);
                break;
            case logical_type::INTEGER:
                value_ = std::get<int32_t>(other.value_);
                break;
            case logical_type::BIGINT:
                value_ = std::get<int64_t>(other.value_);
                break;
            case logical_type::FLOAT:
                value_ = std::get<float>(other.value_);
                break;
            case logical_type::DOUBLE:
                value_ = std::get<double>(other.value_);
                break;
            case logical_type::UTINYINT:
                value_ = std::get<uint8_t>(other.value_);
                break;
            case logical_type::USMALLINT:
                value_ = std::get<uint16_t>(other.value_);
                break;
            case logical_type::UINTEGER:
                value_ = std::get<uint32_t>(other.value_);
                break;
            case logical_type::UBIGINT:
                value_ = std::get<uint64_t>(other.value_);
                break;
            case logical_type::STRING_LITERAL:
                value_ = std::move(std::get<std::unique_ptr<std::string>>(other.value_));
                break;
            case logical_type::POINTER:
                value_ = std::get<void*>(other.value_);
                break;
            case logical_type::LIST:
            case logical_type::ARRAY:
            case logical_type::MAP:
            case logical_type::STRUCT:
                value_ = std::move(std::get<std::unique_ptr<std::vector<logical_value_t>>>(other.value_));
                break;
            default:
                value_ = nullptr;
        }
        return *this;
    }

    const complex_logical_type& logical_value_t::type() const noexcept { return type_; }

    bool logical_value_t::is_null() const noexcept { return type_.type() == logical_type::NA; }

    logical_value_t logical_value_t::cast_as(const complex_logical_type& type) const {
        if (type_ == type) {
            return logical_value_t(*this);
        }

        // TODO: cast to new value
        assert(false && "cast to value is not implemented");
    }

    void logical_value_t::set_alias(const std::string& alias) { type_.set_alias(alias); }

    bool logical_value_t::operator==(const logical_value_t& rhs) const {
        if (type_ == rhs.type_) {
            return true;
        } else {
            switch (type_.type()) {
                case logical_type::BOOLEAN:
                    return std::get<bool>(value_) == std::get<bool>(rhs.value_);
                case logical_type::TINYINT:
                    return std::get<int8_t>(value_) == std::get<int8_t>(rhs.value_);
                case logical_type::SMALLINT:
                    return std::get<int16_t>(value_) == std::get<int16_t>(rhs.value_);
                case logical_type::INTEGER:
                    return std::get<int32_t>(value_) == std::get<int32_t>(rhs.value_);
                case logical_type::BIGINT:
                    return std::get<int64_t>(value_) == std::get<int64_t>(rhs.value_);
                // TODO: use proper floating comparasing
                case logical_type::FLOAT:
                    return std::get<float>(value_) == std::get<float>(rhs.value_);
                case logical_type::DOUBLE:
                    return std::get<double>(value_) == std::get<double>(rhs.value_);
                case logical_type::UTINYINT:
                    return std::get<uint8_t>(value_) == std::get<uint8_t>(rhs.value_);
                case logical_type::USMALLINT:
                    return std::get<uint16_t>(value_) == std::get<uint16_t>(rhs.value_);
                case logical_type::UINTEGER:
                    return std::get<uint32_t>(value_) == std::get<uint32_t>(rhs.value_);
                case logical_type::UBIGINT:
                    return std::get<uint64_t>(value_) == std::get<uint64_t>(rhs.value_);
                case logical_type::STRING_LITERAL:
                    return *std::get<std::unique_ptr<std::string>>(value_) ==
                           *std::get<std::unique_ptr<std::string>>(rhs.value_);
                case logical_type::POINTER:
                    return std::get<void*>(value_) == std::get<void*>(rhs.value_);
                case logical_type::LIST:
                case logical_type::ARRAY:
                case logical_type::MAP:
                case logical_type::STRUCT:
                    return *std::get<std::unique_ptr<std::vector<logical_value_t>>>(value_) ==
                           *std::get<std::unique_ptr<std::vector<logical_value_t>>>(rhs.value_);
                default:
                    assert(false && "unrecognized type");
                    return false;
            }
        }
    }

    bool logical_value_t::operator!=(const logical_value_t& rhs) const { return !(*this == rhs); }

    bool logical_value_t::operator<(const logical_value_t& rhs) const {
        if (type_ != rhs.type_) {
            assert(false && "can not compare types");
            // TODO: add numeric types comparasing
            return false;
        } else {
            switch (type_.type()) {
                case logical_type::BOOLEAN:
                    return std::get<bool>(value_) < std::get<bool>(rhs.value_);
                case logical_type::TINYINT:
                    return std::get<int8_t>(value_) < std::get<int8_t>(rhs.value_);
                case logical_type::SMALLINT:
                    return std::get<int16_t>(value_) < std::get<int16_t>(rhs.value_);
                case logical_type::INTEGER:
                    return std::get<int32_t>(value_) < std::get<int32_t>(rhs.value_);
                case logical_type::BIGINT:
                    return std::get<int64_t>(value_) < std::get<int64_t>(rhs.value_);
                case logical_type::FLOAT:
                    return std::get<float>(value_) < std::get<float>(rhs.value_);
                case logical_type::DOUBLE:
                    return std::get<double>(value_) < std::get<double>(rhs.value_);
                case logical_type::UTINYINT:
                    return std::get<uint8_t>(value_) < std::get<uint8_t>(rhs.value_);
                case logical_type::USMALLINT:
                    return std::get<uint16_t>(value_) < std::get<uint16_t>(rhs.value_);
                case logical_type::UINTEGER:
                    return std::get<uint32_t>(value_) < std::get<uint32_t>(rhs.value_);
                case logical_type::UBIGINT:
                    return std::get<uint64_t>(value_) < std::get<uint64_t>(rhs.value_);
                case logical_type::STRING_LITERAL:
                    return *std::get<std::unique_ptr<std::string>>(value_) <
                           *std::get<std::unique_ptr<std::string>>(rhs.value_);
                default:
                    assert(false && "unrecognized type");
                    return false;
            }
        }
    }

    bool logical_value_t::operator>(const logical_value_t& rhs) const { return rhs < *this; }

    bool logical_value_t::operator<=(const logical_value_t& rhs) const { return !(*this > rhs); }

    bool logical_value_t::operator>=(const logical_value_t& rhs) const { return !(*this < rhs); }

    const std::vector<logical_value_t>& logical_value_t::children() const {
        // TODO: check type
        return *std::get<std::unique_ptr<std::vector<logical_value_t>>>(value_);
    }

    logical_value_t logical_value_t::create_struct(const complex_logical_type& type,
                                                   const std::vector<logical_value_t>& struct_values) {
        logical_value_t result;
        auto child_types = type.child_types();
        result.value_ = std::make_unique<std::vector<logical_value_t>>(struct_values);
        result.type_ = type;
        return result;
    }

    logical_value_t logical_value_t::create_struct(const std::vector<logical_value_t>& fields) {
        std::vector<complex_logical_type> child_types;
        for (auto& child : fields) {
            child_types.push_back(child.type());
        }
        return create_struct(complex_logical_type::create_struct(child_types), fields);
    }

    logical_value_t logical_value_t::create_array(const complex_logical_type& internal_type,
                                                  const std::vector<logical_value_t>& values) {
        auto result = logical_value_t(complex_logical_type::create_array(internal_type, values.size()));
        result.value_ = std::make_unique<std::vector<logical_value_t>>(values);
        return result;
    }

    logical_value_t logical_value_t::create_numeric(const complex_logical_type& type, int64_t value) {
        switch (type.type()) {
            case logical_type::BOOLEAN:
                assert(value == 0 || value == 1);
                return logical_value_t(value ? true : false);
            case logical_type::TINYINT:
                assert(value >= std::numeric_limits<int8_t>::min() && value <= std::numeric_limits<int8_t>::max());
                return logical_value_t((int8_t) value);
            case logical_type::SMALLINT:
                assert(value >= std::numeric_limits<int16_t>::min() && value <= std::numeric_limits<int16_t>::max());
                return logical_value_t((int16_t) value);
            case logical_type::INTEGER:
                assert(value >= std::numeric_limits<int32_t>::min() && value <= std::numeric_limits<int32_t>::max());
                return logical_value_t((int32_t) value);
            case logical_type::BIGINT:
                return logical_value_t(value);
            case logical_type::UTINYINT:
                assert(value >= std::numeric_limits<uint8_t>::min() && value <= std::numeric_limits<uint8_t>::max());
                return logical_value_t((uint8_t) value);
            case logical_type::USMALLINT:
                assert(value >= std::numeric_limits<uint16_t>::min() && value <= std::numeric_limits<uint16_t>::max());
                return logical_value_t((uint16_t) value);
            case logical_type::UINTEGER:
                assert(value >= std::numeric_limits<uint32_t>::min() && value <= std::numeric_limits<uint32_t>::max());
                return logical_value_t((uint32_t) value);
            case logical_type::UBIGINT:
                assert(value >= 0);
                return logical_value_t((uint64_t) value);
            // case logical_type::HUGEINT:
            //     return logical_value_t((int128_t)value);
            // case logical_type::UHUGEINT:
            //     return logical_value_t((uint128_t)value);
            case logical_type::DECIMAL:
                return create_decimal(value,
                                      static_cast<decimal_logical_type_extention*>(type.extention())->width(),
                                      static_cast<decimal_logical_type_extention*>(type.extention())->scale());
            case logical_type::FLOAT:
                return logical_value_t((float) value);
            case logical_type::DOUBLE:
                return logical_value_t((double) value);
            case logical_type::POINTER:
                return logical_value_t(reinterpret_cast<void*>(value));
            default:
                assert(false && "Numeric requires numeric type");
        }
    }

    logical_value_t logical_value_t::create_decimal(int64_t value, uint8_t width, uint8_t scale) {
        auto decimal_type = complex_logical_type::create_decimal(width, scale);
        logical_value_t result(decimal_type);
        result.value_ = value;
        return result;
    }

    logical_value_t logical_value_t::create_map(const complex_logical_type& key_type,
                                                const complex_logical_type& value_type,
                                                const std::vector<logical_value_t>& keys,
                                                const std::vector<logical_value_t>& values) {
        assert(keys.size() == values.size());
        logical_value_t result(complex_logical_type::create_map(key_type, value_type));
        auto keys_value = create_array(key_type, keys);
        auto values_value = create_array(value_type, values);
        result.value_ =
            std::make_unique<std::vector<logical_value_t>>(std::vector{std::move(keys_value), std::move(values_value)});
        return result;
    }

    logical_value_t logical_value_t::create_map(const complex_logical_type& type,
                                                const std::vector<logical_value_t>& values) {
        std::vector<logical_value_t> map_keys;
        std::vector<logical_value_t> map_values;
        for (auto& val : values) {
            assert(val.type().type() == logical_type::STRUCT);
            auto& children = val.children();
            assert(children.size() == 2);
            map_keys.push_back(children[0]);
            map_values.push_back(children[1]);
        }
        auto& key_type = type.child_types()[0];
        auto& value_type = type.child_types()[1];
        return create_map(key_type, value_type, std::move(map_keys), std::move(map_values));
    }

    logical_value_t logical_value_t::create_list(const complex_logical_type& internal_type,
                                                 const std::vector<logical_value_t>& values) {
        logical_value_t result;
        result.type_ = complex_logical_type::create_list(internal_type);
        result.value_ = std::make_unique<std::vector<logical_value_t>>(values);
        return result;
    }
    bool serialize_type_matches(const complex_logical_type& expected_type, const complex_logical_type& actual_type) {
        if (expected_type.type() != actual_type.type()) {
            return false;
        }
        if (expected_type.is_nested()) {
            return true;
        }
        return expected_type == actual_type;
    }

} // namespace components::types