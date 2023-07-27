#pragma once

#include <cassert>
#include <cmath>   // ldexp
#include <cstddef> // size_t
#include <cstdint> // uint8_t, uint16_t, uint32_t, uint64_t
#include <cstdio>  // snprintf
#include <cstring> // memcpy

#include <algorithm> // generate_n
#include <array>     // array
#include <iterator>  // back_inserter
#include <limits>    // numeric_limits
#include <string>    // char_traits, string
#include <utility>   // make_pair, move
#include <vector>    // vector

#include "sax.hpp"
#include "is_sax.hpp"
#include "utils.hpp"

class binary_reader final {

//// Do NOT use this class
//// Testing purposes only

public:
    explicit binary_reader(std::vector<std::uint8_t> binaryDataInputArray) noexcept {
        _binaryData = binaryDataInputArray;
    }

    // make class move-only
    binary_reader(const binary_reader&) = delete;
    binary_reader(binary_reader&&) = default;
    binary_reader& operator=(const binary_reader&) = delete;
    binary_reader& operator=(binary_reader&&) = default;
    ~binary_reader() = default;

    std::string msgpack_parse_to_json() {
        while (parse_internal());
        return _jsonString;
    }

private:
    bool parse_internal() {
        if (!get()) return false;
        switch (_current) {

            case std::char_traits<uint8_t>::eof():
                return unexpect_eof("value");


            case 0x00:
            case 0x01:
            case 0x02:
            case 0x03:
            case 0x04:
            case 0x05:
            case 0x06:
            case 0x07:
            case 0x08:
            case 0x09:
            case 0x0A:
            case 0x0B:
            case 0x0C:
            case 0x0D:
            case 0x0E:
            case 0x0F:
            case 0x10:
            case 0x11:
            case 0x12:
            case 0x13:
            case 0x14:
            case 0x15:
            case 0x16:
            case 0x17:
            case 0x18:
            case 0x19:
            case 0x1A:
            case 0x1B:
            case 0x1C:
            case 0x1D:
            case 0x1E:
            case 0x1F:
            case 0x20:
            case 0x21:
            case 0x22:
            case 0x23:
            case 0x24:
            case 0x25:
            case 0x26:
            case 0x27:
            case 0x28:
            case 0x29:
            case 0x2A:
            case 0x2B:
            case 0x2C:
            case 0x2D:
            case 0x2E:
            case 0x2F:
            case 0x30:
            case 0x31:
            case 0x32:
            case 0x33:
            case 0x34:
            case 0x35:
            case 0x36:
            case 0x37:
            case 0x38:
            case 0x39:
            case 0x3A:
            case 0x3B:
            case 0x3C:
            case 0x3D:
            case 0x3E:
            case 0x3F:
            case 0x40:
            case 0x41:
            case 0x42:
            case 0x43:
            case 0x44:
            case 0x45:
            case 0x46:
            case 0x47:
            case 0x48:
            case 0x49:
            case 0x4A:
            case 0x4B:
            case 0x4C:
            case 0x4D:
            case 0x4E:
            case 0x4F:
            case 0x50:
            case 0x51:
            case 0x52:
            case 0x53:
            case 0x54:
            case 0x55:
            case 0x56:
            case 0x57:
            case 0x58:
            case 0x59:
            case 0x5A:
            case 0x5B:
            case 0x5C:
            case 0x5D:
            case 0x5E:
            case 0x5F:
            case 0x60:
            case 0x61:
            case 0x62:
            case 0x63:
            case 0x64:
            case 0x65:
            case 0x66:
            case 0x67:
            case 0x68:
            case 0x69:
            case 0x6A:
            case 0x6B:
            case 0x6C:
            case 0x6D:
            case 0x6E:
            case 0x6F:
            case 0x70:
            case 0x71:
            case 0x72:
            case 0x73:
            case 0x74:
            case 0x75:
            case 0x76:
            case 0x77:
            case 0x78:
            case 0x79:
            case 0x7A:
            case 0x7B:
            case 0x7C:
            case 0x7D:
            case 0x7E:
            case 0x7F:
                _jsonString.append(std::to_string(_current));
                return true;

            // fixmap
            case 0x80:
            case 0x81:
            case 0x82:
            case 0x83:
            case 0x84:
            case 0x85:
            case 0x86:
            case 0x87:
            case 0x88:
            case 0x89:
            case 0x8A:
            case 0x8B:
            case 0x8C:
            case 0x8D:
            case 0x8E:
            case 0x8F:
                return get_msgpack_object(conditional_static_cast<std::size_t>(static_cast<unsigned int>(_current) & 0x0Fu));

            // fixarray
            case 0x90:
            case 0x91:
            case 0x92:
            case 0x93:
            case 0x94:
            case 0x95:
            case 0x96:
            case 0x97:
            case 0x98:
            case 0x99:
            case 0x9A:
            case 0x9B:
            case 0x9C:
            case 0x9D:
            case 0x9E:
            case 0x9F:
                return get_msgpack_array(conditional_static_cast<std::size_t>(static_cast<unsigned int>(_current) & 0x0Fu));

            // fixstr
            case 0xA0:
            case 0xA1:
            case 0xA2:
            case 0xA3:
            case 0xA4:
            case 0xA5:
            case 0xA6:
            case 0xA7:
            case 0xA8:
            case 0xA9:
            case 0xAA:
            case 0xAB:
            case 0xAC:
            case 0xAD:
            case 0xAE:
            case 0xAF:
            case 0xB0:
            case 0xB1:
            case 0xB2:
            case 0xB3:
            case 0xB4:
            case 0xB5:
            case 0xB6:
            case 0xB7:
            case 0xB8:
            case 0xB9:
            case 0xBA:
            case 0xBB:
            case 0xBC:
            case 0xBD:
            case 0xBE:
            case 0xBF:
            case 0xD9: // str 8
            case 0xDA: // str 16
            case 0xDB: // str 32
            {
                return get_msgpack_string();
            }

            case 0xC0: // nil
                _jsonString.append("null");
                return true;

            case 0xC2: // false
                _jsonString.append("true");
                return true;

            case 0xC3: // true
                _jsonString.append("false");
                return true;

            case 0xC4: // bin 8
            case 0xC5: // bin 16
            case 0xC6: // bin 32
            case 0xC7: // ext 8
            case 0xC8: // ext 16
            case 0xC9: // ext 32
            case 0xD4: // fixext 1
            case 0xD5: // fixext 2
            case 0xD6: // fixext 4
            case 0xD7: // fixext 8
            case 0xD8: // fixext 16
            {
                return get_msgpack_binary();
            }

            case 0xCA: // float 32
            {
                return write_number<float>();
            }

            case 0xCB: // float 64
            {
                return write_number<double>();
            }

            case 0xCC: // uint 8
            {
                return write_number<uint8_t>();
            }

            case 0xCD: // uint 16
            {
                return write_number<uint16_t>();
            }

            case 0xCE: // uint 32
            {
                return write_number<uint32_t>();
            }

            case 0xCF: // uint 64
            {
                return write_number<uint64_t>();
            }

            case 0xD0: // int 8
            {
                return write_number<int8_t>();
            }

            case 0xD1: // int 16
            {
                return write_number<int16_t>();
            }

            case 0xD2: // int 32
            {
                return write_number<int32_t>();
            }

            case 0xD3: // int 64
            {
                return write_number<int64_t>();
            }

            case 0xDC: // array 16
            {
                uint16_t len;
                return get_number<uint16_t>(len) && get_msgpack_array(static_cast<std::size_t>(len));
            }

            case 0xDD: // array 32
            {
                uint32_t len;
                return get_number<uint32_t>(len) && get_msgpack_array(conditional_static_cast<std::size_t>(len));
            }

            case 0xDE: // map 16
            {
                uint16_t len;
                return get_number<uint16_t>(len) && get_msgpack_object(static_cast<std::size_t>(len));
            }

            case 0xDF: // map 32
            {
                uint32_t len;
                return get_number<uint32_t>(len) && get_msgpack_object(conditional_static_cast<std::size_t>(len));
            }

            // negative fixint
            case 0xE0:
            case 0xE1:
            case 0xE2:
            case 0xE3:
            case 0xE4:
            case 0xE5:
            case 0xE6:
            case 0xE7:
            case 0xE8:
            case 0xE9:
            case 0xEA:
            case 0xEB:
            case 0xEC:
            case 0xED:
            case 0xEE:
            case 0xEF:
            case 0xF0:
            case 0xF1:
            case 0xF2:
            case 0xF3:
            case 0xF4:
            case 0xF5:
            case 0xF6:
            case 0xF7:
            case 0xF8:
            case 0xF9:
            case 0xFA:
            case 0xFB:
            case 0xFC:
            case 0xFD:
            case 0xFE:
            case 0xFF:
                return write_number<std::int8_t>();

            default: // anything else
            {
                auto last_token = get_token_string();
                //return sax->parse_error(chars_read, last_token, parse_error::create(112, chars_read, exception_message(concat("invalid byte: 0x", last_token), "value"), nullptr));
                return false;
            }
        }
    }
    bool get_msgpack_string() {
        if (HEDLEY_UNLIKELY(!unexpect_eof("string"))) {
            return false;
        }

        switch (_current) {
            // fixstr
            case 0xA0:
            case 0xA1:
            case 0xA2:
            case 0xA3:
            case 0xA4:
            case 0xA5:
            case 0xA6:
            case 0xA7:
            case 0xA8:
            case 0xA9:
            case 0xAA:
            case 0xAB:
            case 0xAC:
            case 0xAD:
            case 0xAE:
            case 0xAF:
            case 0xB0:
            case 0xB1:
            case 0xB2:
            case 0xB3:
            case 0xB4:
            case 0xB5:
            case 0xB6:
            case 0xB7:
            case 0xB8:
            case 0xB9:
            case 0xBA:
            case 0xBB:
            case 0xBC:
            case 0xBD:
            case 0xBE:
            case 0xBF: {
                return write_string(static_cast<unsigned int>(_current) & 0x1Fu);
            }

            case 0xD9: // str 8
            {
                std::uint8_t len{};
                return get_number<uint8_t>(len) && write_string(len);
            }

            case 0xDA: // str 16
            {
                std::uint16_t len{};
                return get_number<uint16_t>(len) && write_string(len);
            }

            case 0xDB: // str 32
            {
                std::uint32_t len{};
                return get_number<uint32_t>(len) && write_string(len);
            }

            default: {
                auto last_token = get_token_string();
                //return sax->parse_error(chars_read, last_token, parse_error::create(113, chars_read, exception_message(concat("expected length specification (0xA0-0xBF, 0xD9-0xDB); last byte: 0x", last_token), "string"), nullptr));
                return false;
            }
        }
    }
    bool get_msgpack_binary() {
        std::vector<uint8_t> result;
        // helper function to set the subtype
        auto assign_and_return_true = [&result](std::int8_t subtype) {
            //result.set_subtype(static_cast<std::uint8_t>(subtype));
            return true;
        };

        switch (_current) {
            case 0xC4: // bin 8
            {
                std::uint8_t len{};
                return get_number<uint8_t>(len) &&
                       get_binary(len);
            }

            case 0xC5: // bin 16
            {
                std::uint16_t len{};
                return get_number<uint16_t>(len) &&
                       get_binary(len);
            }

            case 0xC6: // bin 32
            {
                std::uint32_t len{};
                return get_number<uint32_t>(len) &&
                       get_binary(len);
            }

            case 0xC7: // ext 8
            {
                std::uint8_t len{};
                std::int8_t subtype{};
                return get_number(len) &&
                       get_number(subtype) &&
                       get_binary(len) &&
                       assign_and_return_true(subtype);
            }

            case 0xC8: // ext 16
            {
                std::uint16_t len{};
                std::int8_t subtype{};
                return get_number(len) &&
                       get_number(subtype) &&
                       get_binary(len) &&
                       assign_and_return_true(subtype);
            }

            case 0xC9: // ext 32
            {
                std::uint32_t len{};
                std::int8_t subtype{};
                return get_number(len) &&
                       get_number(subtype) &&
                       get_binary(len) &&
                       assign_and_return_true(subtype);
            }

            case 0xD4: // fixext 1
            {
                std::int8_t subtype{};
                return get_number(subtype) &&
                       get_binary(1) &&
                       assign_and_return_true(subtype);
            }

            case 0xD5: // fixext 2
            {
                std::int8_t subtype{};
                return get_number(subtype) &&
                       get_binary(2) &&
                       assign_and_return_true(subtype);
            }

            case 0xD6: // fixext 4
            {
                std::int8_t subtype{};
                return get_number(subtype) &&
                       get_binary(4) &&
                       assign_and_return_true(subtype);
            }

            case 0xD7: // fixext 8
            {
                std::int8_t subtype{};
                return get_number(subtype) &&
                       get_binary(8) &&
                       assign_and_return_true(subtype);
            }

            case 0xD8: // fixext 16
            {
                std::int8_t subtype{};
                return get_number(subtype) &&
                       get_binary(16) &&
                       assign_and_return_true(subtype);
            }

            default:          // LCOV_EXCL_LINE
                return false; // LCOV_EXCL_LINE
        }
    }
    bool get_msgpack_array(const std::size_t len) {

        _jsonString.append("[");

        for (std::size_t i = 0; i < len; ++i) {
            if (!parse_internal()) {
                return false;
            }
            if (i != (len - 1)) _jsonString.append(",");
        }

        _jsonString.append("]");
        return true;
    }

    bool get_msgpack_object(const std::size_t len) {

        _jsonString.append("{");


        for (std::size_t i = 0; i < len; ++i) {
            get(); // maybe move it to @get_msgpack_string@ later

            // key
            if (HEDLEY_UNLIKELY(!get_msgpack_string())) {
                return false;
            }

            //
            _jsonString.append(":");
            //

            // value
            if (HEDLEY_UNLIKELY(!parse_internal())) {
                return false;
            }

            if (i != (len - 1)) _jsonString.append(",");
        }

        _jsonString.append("}");
        return true;
    }

    uint8_t get() {
        ++chars_read;
        return pop_front();
    }

    uint8_t pop_front()
    {
        if (_binaryData.empty()) return false;
        else {
            _current = *_binaryData.begin();
            _binaryData.erase(_binaryData.begin());
            return true;
        }
        
    }

    uint8_t get_ignore_noop() {
        do {
            get();
        } while (_current == 'N');

        return _current;
    }

    template<typename NumberType, bool InputIsLittleEndian = false>
    bool write_number() {
        NumberType result;
        // step 1: read input into array with system's byte order
        std::array<std::uint8_t, sizeof(NumberType)> vec{};
        for (std::size_t i = 0; i < sizeof(NumberType); ++i) {
            get();
            if (HEDLEY_UNLIKELY(!unexpect_eof("number"))) {
                return false;
            }

            // reverse byte order prior to conversion if necessary
            if (is_little_endian != (InputIsLittleEndian)) {
                vec[sizeof(NumberType) - i - 1] = static_cast<std::uint8_t>(_current);
            } else {
                vec[i] = static_cast<std::uint8_t>(_current); // LCOV_EXCL_LINE
            }
        }

        // step 2: convert array into number of type T and return
        std::memcpy(&result, vec.data(), sizeof(NumberType));

        _jsonString.append(std::to_string(result));
        return true;
    }

    template<typename NumberType, bool InputIsLittleEndian = false>
    bool get_number(NumberType &result) {
        // step 1: read input into array with system's byte order
        std::array<std::uint8_t, sizeof(NumberType)> vec{};
        for (std::size_t i = 0; i < sizeof(NumberType); ++i) {
            get();
            if (HEDLEY_UNLIKELY(!unexpect_eof("number"))) {
                return false;
            }

            // reverse byte order prior to conversion if necessary
            if (is_little_endian != (InputIsLittleEndian)) {
                vec[sizeof(NumberType) - i - 1] = static_cast<std::uint8_t>(_current);
            } else {
                vec[i] = static_cast<std::uint8_t>(_current); // LCOV_EXCL_LINE
            }
        }

        // step 2: convert array into number of type T and return
        std::memcpy(&result, vec.data(), sizeof(NumberType));
        return true;
    }

    template<typename NumberType>
    bool write_string(const NumberType len) {
        std::string result;
        bool success = true;
        for (NumberType i = 0; i < len; i++) {
            get();
            if (HEDLEY_UNLIKELY(!unexpect_eof("string"))) {
                success = false;
                break;
            }
            result.push_back(static_cast<uint8_t>(_current));
        }
        _jsonString.push_back('"');
        _jsonString.append(result);
        _jsonString.push_back('"');
        return success;
    }



    template<typename NumberType>
    bool get_binary(const NumberType len) {
        bool success = true;
        std::vector<uint8_t> result;
        for (NumberType i = 0; i < len; i++) {
            get();
            if (HEDLEY_UNLIKELY(!unexpect_eof("binary"))) {
                success = false;
                break;
            }
            result.push_back(static_cast<std::uint8_t>(_current));
        }
        _jsonString.append(std::string(result.begin(), result.end())); // there is a better way to do this!
        return success;
    }
    bool unexpect_eof(const char* context) const {
        if (HEDLEY_UNLIKELY(_current == std::char_traits<uint8_t>::eof())) {
            //return sax->parse_error(chars_read, "<end of file>", parse_error::create(110, chars_read, exception_message("unexpected end of input", context), nullptr));
            return false;
        }
        return true;
    }
    std::string get_token_string() const {
        std::array<char, 3> cr{{}};
        static_cast<void>((std::snprintf)(cr.data(), cr.size(), "%.2hhX", static_cast<unsigned char>(_current))); // NOLINT(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
        return std::string{cr.data()};
    }
    std::string exception_message(const std::string& detail, const std::string& context) const {
        std::string error_msg = "syntax error while parsing ";
        return error_msg + ' ' + context + ": " + detail;
    }

private:
    std::vector<std::uint8_t> _binaryData;

    std::string _jsonString = "";

    uint8_t _current = std::char_traits<uint8_t>::eof();

    std::size_t chars_read  = 0;

    const bool is_little_endian = little_endianness();

};

///////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////


class binary_reader_native  final {
   
public:
    explicit binary_reader_native(std::vector<std::uint8_t> binaryDataInputArray) noexcept {
        _binaryData = binaryDataInputArray;
    }

    // make class move-only
    binary_reader_native(const binary_reader_native&) = delete;
    binary_reader_native(binary_reader_native&&) = default;
    binary_reader_native& operator=(const binary_reader_native&) = delete;
    binary_reader_native& operator=(binary_reader_native&&) = default;
    ~binary_reader_native() = default;

    std::string msgpack_parse_to_json() {
        while (parse_main_datatypes());
        return _jsonString;
    }

private:
    bool parse_main_datatypes(){
        if (!get()) return false;
        switch (_current) {
            // fixmap
            case 0x80:
            case 0x81:
            case 0x82:
            case 0x83:
            case 0x84:
            case 0x85:
            case 0x86:
            case 0x87:
            case 0x88:
            case 0x89:
            case 0x8A:
            case 0x8B:
            case 0x8C:
            case 0x8D:
            case 0x8E:
            case 0x8F:
                return get_msgpack_object(conditional_static_cast<std::size_t>(static_cast<unsigned int>(_current) & 0x0Fu));

            // fixarray
            case 0x90:
            case 0x91:
            case 0x92:
            case 0x93:
            case 0x94:
            case 0x95:
            case 0x96:
            case 0x97:
            case 0x98:
            case 0x99:
            case 0x9A:
            case 0x9B:
            case 0x9C:
            case 0x9D:
            case 0x9E:
            case 0x9F:
                return get_msgpack_array(conditional_static_cast<std::size_t>(static_cast<unsigned int>(_current) & 0x0Fu));
            case 0xDC: // array 16
            {
                uint16_t len;
                return get_number<uint16_t>(len) && get_msgpack_array(static_cast<std::size_t>(len));
            }

            case 0xDD: // array 32
            {
                uint32_t len;
                return get_number<uint32_t>(len) && get_msgpack_array(conditional_static_cast<std::size_t>(len));
            }

            case 0xDE: // map 16
            {
                uint16_t len;
                return get_number<uint16_t>(len) && get_msgpack_object(static_cast<std::size_t>(len));
            }

            case 0xDF: // map 32
            {
                uint32_t len;
                return get_number<uint32_t>(len) && get_msgpack_object(conditional_static_cast<std::size_t>(len));
            }

        }
    }

    bool parse_internal() {
        if (!get()) return false;
        switch (_current) {

            case std::char_traits<uint8_t>::eof():
                return unexpect_eof("value");


            case 0x00:
            case 0x01:
            case 0x02:
            case 0x03:
            case 0x04:
            case 0x05:
            case 0x06:
            case 0x07:
            case 0x08:
            case 0x09:
            case 0x0A:
            case 0x0B:
            case 0x0C:
            case 0x0D:
            case 0x0E:
            case 0x0F:
            case 0x10:
            case 0x11:
            case 0x12:
            case 0x13:
            case 0x14:
            case 0x15:
            case 0x16:
            case 0x17:
            case 0x18:
            case 0x19:
            case 0x1A:
            case 0x1B:
            case 0x1C:
            case 0x1D:
            case 0x1E:
            case 0x1F:
            case 0x20:
            case 0x21:
            case 0x22:
            case 0x23:
            case 0x24:
            case 0x25:
            case 0x26:
            case 0x27:
            case 0x28:
            case 0x29:
            case 0x2A:
            case 0x2B:
            case 0x2C:
            case 0x2D:
            case 0x2E:
            case 0x2F:
            case 0x30:
            case 0x31:
            case 0x32:
            case 0x33:
            case 0x34:
            case 0x35:
            case 0x36:
            case 0x37:
            case 0x38:
            case 0x39:
            case 0x3A:
            case 0x3B:
            case 0x3C:
            case 0x3D:
            case 0x3E:
            case 0x3F:
            case 0x40:
            case 0x41:
            case 0x42:
            case 0x43:
            case 0x44:
            case 0x45:
            case 0x46:
            case 0x47:
            case 0x48:
            case 0x49:
            case 0x4A:
            case 0x4B:
            case 0x4C:
            case 0x4D:
            case 0x4E:
            case 0x4F:
            case 0x50:
            case 0x51:
            case 0x52:
            case 0x53:
            case 0x54:
            case 0x55:
            case 0x56:
            case 0x57:
            case 0x58:
            case 0x59:
            case 0x5A:
            case 0x5B:
            case 0x5C:
            case 0x5D:
            case 0x5E:
            case 0x5F:
            case 0x60:
            case 0x61:
            case 0x62:
            case 0x63:
            case 0x64:
            case 0x65:
            case 0x66:
            case 0x67:
            case 0x68:
            case 0x69:
            case 0x6A:
            case 0x6B:
            case 0x6C:
            case 0x6D:
            case 0x6E:
            case 0x6F:
            case 0x70:
            case 0x71:
            case 0x72:
            case 0x73:
            case 0x74:
            case 0x75:
            case 0x76:
            case 0x77:
            case 0x78:
            case 0x79:
            case 0x7A:
            case 0x7B:
            case 0x7C:
            case 0x7D:
            case 0x7E:
            case 0x7F:
                _jsonString.append(std::to_string(_current));
                return true;

            // fixmap
            case 0x80:
            case 0x81:
            case 0x82:
            case 0x83:
            case 0x84:
            case 0x85:
            case 0x86:
            case 0x87:
            case 0x88:
            case 0x89:
            case 0x8A:
            case 0x8B:
            case 0x8C:
            case 0x8D:
            case 0x8E:
            case 0x8F:
                return get_msgpack_object(conditional_static_cast<std::size_t>(static_cast<unsigned int>(_current) & 0x0Fu));

            // fixarray
            case 0x90:
            case 0x91:
            case 0x92:
            case 0x93:
            case 0x94:
            case 0x95:
            case 0x96:
            case 0x97:
            case 0x98:
            case 0x99:
            case 0x9A:
            case 0x9B:
            case 0x9C:
            case 0x9D:
            case 0x9E:
            case 0x9F:
                return get_msgpack_array(conditional_static_cast<std::size_t>(static_cast<unsigned int>(_current) & 0x0Fu));

            // fixstr
            case 0xA0:
            case 0xA1:
            case 0xA2:
            case 0xA3:
            case 0xA4:
            case 0xA5:
            case 0xA6:
            case 0xA7:
            case 0xA8:
            case 0xA9:
            case 0xAA:
            case 0xAB:
            case 0xAC:
            case 0xAD:
            case 0xAE:
            case 0xAF:
            case 0xB0:
            case 0xB1:
            case 0xB2:
            case 0xB3:
            case 0xB4:
            case 0xB5:
            case 0xB6:
            case 0xB7:
            case 0xB8:
            case 0xB9:
            case 0xBA:
            case 0xBB:
            case 0xBC:
            case 0xBD:
            case 0xBE:
            case 0xBF:
            case 0xD9: // str 8
            case 0xDA: // str 16
            case 0xDB: // str 32
            {
                return get_msgpack_string();
            }

            case 0xC0: // nil
                _jsonString.append("null");
                return true;

            case 0xC2: // false
                _jsonString.append("true");
                return true;

            case 0xC3: // true
                _jsonString.append("false");
                return true;

            case 0xC4: // bin 8
            case 0xC5: // bin 16
            case 0xC6: // bin 32
            case 0xC7: // ext 8
            case 0xC8: // ext 16
            case 0xC9: // ext 32
            case 0xD4: // fixext 1
            case 0xD5: // fixext 2
            case 0xD6: // fixext 4
            case 0xD7: // fixext 8
            case 0xD8: // fixext 16
            {
                return get_msgpack_binary();
            }

            case 0xCA: // float 32
            {
                return write_number<float>();
            }

            case 0xCB: // float 64
            {
                return write_number<double>();
            }

            case 0xCC: // uint 8
            {
                return write_number<uint8_t>();
            }

            case 0xCD: // uint 16
            {
                return write_number<uint16_t>();
            }

            case 0xCE: // uint 32
            {
                return write_number<uint32_t>();
            }

            case 0xCF: // uint 64
            {
                return write_number<uint64_t>();
            }

            case 0xD0: // int 8
            {
                return write_number<int8_t>();
            }

            case 0xD1: // int 16
            {
                return write_number<int16_t>();
            }

            case 0xD2: // int 32
            {
                return write_number<int32_t>();
            }

            case 0xD3: // int 64
            {
                return write_number<int64_t>();
            }

            case 0xDC: // array 16
            {
                uint16_t len;
                return get_number<uint16_t>(len) && get_msgpack_array(static_cast<std::size_t>(len));
            }

            case 0xDD: // array 32
            {
                uint32_t len;
                return get_number<uint32_t>(len) && get_msgpack_array(conditional_static_cast<std::size_t>(len));
            }

            case 0xDE: // map 16
            {
                uint16_t len;
                return get_number<uint16_t>(len) && get_msgpack_object(static_cast<std::size_t>(len));
            }

            case 0xDF: // map 32
            {
                uint32_t len;
                return get_number<uint32_t>(len) && get_msgpack_object(conditional_static_cast<std::size_t>(len));
            }

            // negative fixint
            case 0xE0:
            case 0xE1:
            case 0xE2:
            case 0xE3:
            case 0xE4:
            case 0xE5:
            case 0xE6:
            case 0xE7:
            case 0xE8:
            case 0xE9:
            case 0xEA:
            case 0xEB:
            case 0xEC:
            case 0xED:
            case 0xEE:
            case 0xEF:
            case 0xF0:
            case 0xF1:
            case 0xF2:
            case 0xF3:
            case 0xF4:
            case 0xF5:
            case 0xF6:
            case 0xF7:
            case 0xF8:
            case 0xF9:
            case 0xFA:
            case 0xFB:
            case 0xFC:
            case 0xFD:
            case 0xFE:
            case 0xFF:
                return write_number<std::int8_t>();

            default: // anything else
            {
                auto last_token = get_token_string();
                //return sax->parse_error(chars_read, last_token, parse_error::create(112, chars_read, exception_message(concat("invalid byte: 0x", last_token), "value"), nullptr));
                return false;
            }
        }
    }
    bool get_msgpack_string() {
        if (HEDLEY_UNLIKELY(!unexpect_eof("string"))) {
            return false;
        }

        switch (_current) {
            // fixstr
            case 0xA0:
            case 0xA1:
            case 0xA2:
            case 0xA3:
            case 0xA4:
            case 0xA5:
            case 0xA6:
            case 0xA7:
            case 0xA8:
            case 0xA9:
            case 0xAA:
            case 0xAB:
            case 0xAC:
            case 0xAD:
            case 0xAE:
            case 0xAF:
            case 0xB0:
            case 0xB1:
            case 0xB2:
            case 0xB3:
            case 0xB4:
            case 0xB5:
            case 0xB6:
            case 0xB7:
            case 0xB8:
            case 0xB9:
            case 0xBA:
            case 0xBB:
            case 0xBC:
            case 0xBD:
            case 0xBE:
            case 0xBF: {
                return write_string(static_cast<unsigned int>(_current) & 0x1Fu);
            }

            case 0xD9: // str 8
            {
                std::uint8_t len{};
                return get_number<uint8_t>(len) && write_string(len);
            }

            case 0xDA: // str 16
            {
                std::uint16_t len{};
                return get_number<uint16_t>(len) && write_string(len);
            }

            case 0xDB: // str 32
            {
                std::uint32_t len{};
                return get_number<uint32_t>(len) && write_string(len);
            }

            default: {
                auto last_token = get_token_string();
                //return sax->parse_error(chars_read, last_token, parse_error::create(113, chars_read, exception_message(concat("expected length specification (0xA0-0xBF, 0xD9-0xDB); last byte: 0x", last_token), "string"), nullptr));
                return false;
            }
        }
    }
    bool get_msgpack_binary() {
        std::vector<uint8_t> result;
        // helper function to set the subtype
        auto assign_and_return_true = [&result](std::int8_t subtype) {
            //result.set_subtype(static_cast<std::uint8_t>(subtype));
            return true;
        };

        switch (_current) {
            case 0xC4: // bin 8
            {
                std::uint8_t len{};
                return get_number<uint8_t>(len) &&
                       get_binary(len);
            }

            case 0xC5: // bin 16
            {
                std::uint16_t len{};
                return get_number<uint16_t>(len) &&
                       get_binary(len);
            }

            case 0xC6: // bin 32
            {
                std::uint32_t len{};
                return get_number<uint32_t>(len) &&
                       get_binary(len);
            }

            case 0xC7: // ext 8
            {
                std::uint8_t len{};
                std::int8_t subtype{};
                return get_number(len) &&
                       get_number(subtype) &&
                       get_binary(len) &&
                       assign_and_return_true(subtype);
            }

            case 0xC8: // ext 16
            {
                std::uint16_t len{};
                std::int8_t subtype{};
                return get_number(len) &&
                       get_number(subtype) &&
                       get_binary(len) &&
                       assign_and_return_true(subtype);
            }

            case 0xC9: // ext 32
            {
                std::uint32_t len{};
                std::int8_t subtype{};
                return get_number(len) &&
                       get_number(subtype) &&
                       get_binary(len) &&
                       assign_and_return_true(subtype);
            }

            case 0xD4: // fixext 1
            {
                std::int8_t subtype{};
                return get_number(subtype) &&
                       get_binary(1) &&
                       assign_and_return_true(subtype);
            }

            case 0xD5: // fixext 2
            {
                std::int8_t subtype{};
                return get_number(subtype) &&
                       get_binary(2) &&
                       assign_and_return_true(subtype);
            }

            case 0xD6: // fixext 4
            {
                std::int8_t subtype{};
                return get_number(subtype) &&
                       get_binary(4) &&
                       assign_and_return_true(subtype);
            }

            case 0xD7: // fixext 8
            {
                std::int8_t subtype{};
                return get_number(subtype) &&
                       get_binary(8) &&
                       assign_and_return_true(subtype);
            }

            case 0xD8: // fixext 16
            {
                std::int8_t subtype{};
                return get_number(subtype) &&
                       get_binary(16) &&
                       assign_and_return_true(subtype);
            }

            default:          // LCOV_EXCL_LINE
                return false; // LCOV_EXCL_LINE
        }
    }
    bool get_msgpack_array(const std::size_t len) {

        _jsonString.append("[");

        for (std::size_t i = 0; i < len; ++i) {
            if (!parse_internal()) {
                return false;
            }
            if (i != (len - 1)) _jsonString.append(",");
        }

        _jsonString.append("]");
        return true;
    }

    bool get_msgpack_object(const std::size_t len) {

        _jsonString.append("{");


        for (std::size_t i = 0; i < len; ++i) {
            get(); // maybe move it to @get_msgpack_string@ later

            // key
            if (HEDLEY_UNLIKELY(!get_msgpack_string())) {
                return false;
            }

            //
            _jsonString.append(":");
            //

            // value
            if (HEDLEY_UNLIKELY(!parse_internal())) {
                return false;
            }

            if (i != (len - 1)) _jsonString.append(",");
        }

        _jsonString.append("}");
        return true;
    }

    uint8_t get() {
        ++chars_read;
        return pop_front();
    }

    uint8_t pop_front()
    {
        if (_binaryData.empty()) return false;
        else {
            _current = *_binaryData.begin();
            _binaryData.erase(_binaryData.begin());
            return true;
        }
        
    }

    uint8_t get_ignore_noop() {
        do {
            get();
        } while (_current == 'N');

        return _current;
    }

    template<typename NumberType, bool InputIsLittleEndian = false>
    bool write_number() {
        NumberType result;
        // step 1: read input into array with system's byte order
        std::array<std::uint8_t, sizeof(NumberType)> vec{};
        for (std::size_t i = 0; i < sizeof(NumberType); ++i) {
            get();
            if (HEDLEY_UNLIKELY(!unexpect_eof("number"))) {
                return false;
            }

            // reverse byte order prior to conversion if necessary
            if (is_little_endian != (InputIsLittleEndian)) {
                vec[sizeof(NumberType) - i - 1] = static_cast<std::uint8_t>(_current);
            } else {
                vec[i] = static_cast<std::uint8_t>(_current); // LCOV_EXCL_LINE
            }
        }

        // step 2: convert array into number of type T and return
        std::memcpy(&result, vec.data(), sizeof(NumberType));

        _jsonString.append(std::to_string(result));
        return true;
    }

    template<typename NumberType, bool InputIsLittleEndian = false>
    bool get_number(NumberType &result) {
        // step 1: read input into array with system's byte order
        std::array<std::uint8_t, sizeof(NumberType)> vec{};
        for (std::size_t i = 0; i < sizeof(NumberType); ++i) {
            get();
            if (HEDLEY_UNLIKELY(!unexpect_eof("number"))) {
                return false;
            }

            // reverse byte order prior to conversion if necessary
            if (is_little_endian != (InputIsLittleEndian)) {
                vec[sizeof(NumberType) - i - 1] = static_cast<std::uint8_t>(_current);
            } else {
                vec[i] = static_cast<std::uint8_t>(_current); // LCOV_EXCL_LINE
            }
        }

        // step 2: convert array into number of type T and return
        std::memcpy(&result, vec.data(), sizeof(NumberType));
        return true;
    }

    template<typename NumberType>
    bool write_string(const NumberType len) {
        std::string result;
        bool success = true;
        for (NumberType i = 0; i < len; i++) {
            get();
            if (HEDLEY_UNLIKELY(!unexpect_eof("string"))) {
                success = false;
                break;
            }
            result.push_back(static_cast<uint8_t>(_current));
        }
        _jsonString.push_back('"');
        _jsonString.append(result);
        _jsonString.push_back('"');
        return success;
    }



    template<typename NumberType>
    bool get_binary(const NumberType len) {
        bool success = true;
        std::vector<uint8_t> result;
        for (NumberType i = 0; i < len; i++) {
            get();
            if (HEDLEY_UNLIKELY(!unexpect_eof("binary"))) {
                success = false;
                break;
            }
            result.push_back(static_cast<std::uint8_t>(_current));
        }
        _jsonString.append(std::string(result.begin(), result.end())); // there is a better way to do this!
        return success;
    }
    bool unexpect_eof(const char* context) const {
        if (HEDLEY_UNLIKELY(_current == std::char_traits<uint8_t>::eof())) {
            //return sax->parse_error(chars_read, "<end of file>", parse_error::create(110, chars_read, exception_message("unexpected end of input", context), nullptr));
            return false;
        }
        return true;
    }
    std::string get_token_string() const {
        std::array<char, 3> cr{{}};
        static_cast<void>((std::snprintf)(cr.data(), cr.size(), "%.2hhX", static_cast<unsigned char>(_current))); // NOLINT(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
        return std::string{cr.data()};
    }
    std::string exception_message(const std::string& detail, const std::string& context) const {
        std::string error_msg = "syntax error while parsing ";
        return error_msg + ' ' + context + ": " + detail;
    }

private:
    std::vector<std::uint8_t> _binaryData;

    std::string _jsonString = "";

    uint8_t _current = std::char_traits<uint8_t>::eof();

    std::size_t chars_read  = 0;

    const bool is_little_endian = little_endianness();

};