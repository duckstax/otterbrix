#pragma once

#include <cassert>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "utils.hpp"
#include "output_adapters.hpp"
#include <components/document/document_view.hpp>


template<typename CharType>
using output_adapter_t = std::shared_ptr<output_adapter_protocol<CharType>>;

template<typename document_ptr, typename CharType>
class binary_writer {
    using string_t = std::string;
    // using binary_t = byte_container_t;  # document::impl::value_type::data
    using number_double_t = double;

public:
    explicit binary_writer(output_adapter_t<CharType> adapter)
        : oa(std::move(adapter)) {
        assert(oa);
    }

    void DEBUG_FUNCTION(int c){
        //std::cout << ( c >= (std::numeric_limits<std::int32_t>::min)() && c <= (std::numeric_limits<std::int32_t>::max)());
        return;
    }

    void write_simple_datatype(const document::impl::value_t* v) {
        switch (v->type()) {
            case document::impl::value_type::null:
            {
                oa->write_character(to_char_type(0xC0));
                break;
            }

            case document::impl::value_type::boolean: // true and false
            {
                oa->write_character(v->as_bool()
                                        ? to_char_type(0xC3)
                                        : to_char_type(0xC2));
                break;
            }
            
            case document::impl::value_type::number: {
                auto debug = v->as_int();
                auto debug2 = v->as_unsigned();
                DEBUG_FUNCTION(v->as_int());
                if (v->is_unsigned()){
                    if (v->as_unsigned() <= (std::numeric_limits<std::uint8_t>::max)()) {
                        // uint 8
                        oa->write_character(to_char_type(0xCC));
                        write_number(static_cast<std::uint8_t>(v->as_unsigned()));
                    } else if (v->as_unsigned() <= (std::numeric_limits<std::uint16_t>::max)()) {
                        // uint 16
                        oa->write_character(to_char_type(0xCD));
                        write_number(static_cast<std::uint16_t>(v->as_unsigned()));
                    } else if (v->as_unsigned() <= (std::numeric_limits<std::uint32_t>::max)()) {
                        // uint 32
                        oa->write_character(to_char_type(0xCE));
                        write_number(static_cast<std::uint32_t>(v->as_unsigned()));
                    } else if (v->as_unsigned() <= (std::numeric_limits<std::uint64_t>::max)()) {
                        // uint 64
                        oa->write_character(to_char_type(0xCF));
                        write_number(static_cast<std::uint64_t>(v->as_unsigned()));
                    }
                } 
                else if (v->is_int()){
                    if (v->as_int() >= -32 && v->as_int() <= 127) {
                        // fixnum
                        write_number(static_cast<std::int8_t>(v->as_int()));
                    } else if ( v->as_int() >= (std::numeric_limits<std::int8_t>::min)() &&
                                v->as_int() <= (std::numeric_limits<std::int8_t>::max)()) {
                        // int 8
                        oa->write_character(to_char_type(0xD0));
                        write_number(static_cast<std::int8_t>(v->as_int()));
                    } else if ( v->as_int() >= (std::numeric_limits<std::int16_t>::min)() &&
                                v->as_int() <= (std::numeric_limits<std::int16_t>::max)()) {
                        // int 16
                        oa->write_character(to_char_type(0xD1));
                        write_number(static_cast<std::int16_t>(v->as_int()));
                    } else if ( v->as_int() >= (std::numeric_limits<std::int32_t>::min)() &&
                                v->as_int() <= (std::numeric_limits<std::int32_t>::max)()) {
                        // int 32
                        oa->write_character(to_char_type(0xD2));
                        write_number(static_cast<std::int32_t>(v->as_int()));
                    } else if ( v->as_int() >= (std::numeric_limits<std::int64_t>::min)() &&
                                v->as_int() <= (std::numeric_limits<std::int64_t>::max)()) {
                        // int 64
                        oa->write_character(to_char_type(0xD3));
                        write_number(static_cast<std::int64_t>(v->as_int()));
                    }
                } 
                else if (v->is_double()) {
                    write_double(v->as_double());
                }
                break;
            }

            case document::impl::value_type::string: {
                // step 1: write control byte and the string length
                const auto N = v->as_string().size();
                if (N <= 31) {
                    // fixstr
                    write_number(static_cast<std::uint8_t>(0xA0 | N));
                } else if (N <= (std::numeric_limits<std::uint8_t>::max)()) {
                    // str 8
                    oa->write_character(to_char_type(0xD9));
                    write_number(static_cast<std::uint8_t>(N));
                } else if (N <= (std::numeric_limits<std::uint16_t>::max)()) {
                    // str 16
                    oa->write_character(to_char_type(0xDA));
                    write_number(static_cast<std::uint16_t>(N));
                } else if (N <= (std::numeric_limits<std::uint32_t>::max)()) {
                    // str 32
                    oa->write_character(to_char_type(0xDB));
                    write_number(static_cast<std::uint32_t>(N));
                }

                // step 2: write the string
                oa->write_characters(
                    reinterpret_cast<const CharType*>(v->as_string().data()),
                    N);
                break;
            }
            case document::impl::value_type::undefined:
            default:
                break;
        }
    }

    void write_msgpack(const document_ptr& ptr) {
        components::document::document_view_t j(ptr);

        if (!j.is_valid()) return;

        switch (j.get_value()->type()) {
            case document::impl::value_type::null:
            case document::impl::value_type::boolean:
            case document::impl::value_type::number:
            case document::impl::value_type::string:
            {
                write_simple_datatype(j.get_value());
                break;
            }
            case document::impl::value_type::array: {

                // step 1: write control byte and the array size

                const auto N = j.get_value()->as_array()->count();
                if (N <= 15) {
                    // fixarray
                    write_number(static_cast<std::uint8_t>(0x90 | N));
                } else if (N <= (std::numeric_limits<std::uint16_t>::max)()) {
                    // array 16
                    oa->write_character(to_char_type(0xDC));
                    write_number(static_cast<std::uint16_t>(N));
                } else if (N <= (std::numeric_limits<std::uint32_t>::max)()) {
                    // array 32
                    oa->write_character(to_char_type(0xDD));
                    write_number(static_cast<std::uint32_t>(N));
                }

                // step 2: write each element
                for (uint32_t i = 0; i < j.get_value()->as_array()->count(); ++i) {
                    write_simple_datatype(j.get_value()->as_array()->get(i));
                }
                break;
            }
            /*
            case document::impl::value_type::data: { // DATA?
                // step 0: determine if the binary type has a set subtype to
                // determine whether or not to use the ext or fixext types
                const bool use_ext = j.m_value.binary->has_subtype();

                // step 1: write control byte and the byte string length
                const auto N = j.m_value.binary->size();
                if (N <= (std::numeric_limits<std::uint8_t>::max)()) {
                    std::uint8_t output_type{};
                    bool fixed = true;
                    if (use_ext) {
                        switch (N) {
                            case 1:
                                output_type = 0xD4; // fixext 1
                                break;
                            case 2:
                                output_type = 0xD5; // fixext 2
                                break;
                            case 4:
                                output_type = 0xD6; // fixext 4
                                break;
                            case 8:
                                output_type = 0xD7; // fixext 8
                                break;
                            case 16:
                                output_type = 0xD8; // fixext 16
                                break;
                            default:
                                output_type = 0xC7; // ext 8
                                fixed = false;
                                break;
                        }

                    } else {
                        output_type = 0xC4; // bin 8
                        fixed = false;
                    }

                    oa->write_character(to_char_type(output_type));
                    if (!fixed) {
                        write_number(static_cast<std::uint8_t>(N));
                    }
                } else if (N <= (std::numeric_limits<std::uint16_t>::max)()) {
                    const std::uint8_t output_type = use_ext
                                                         ? 0xC8  // ext 16
                                                         : 0xC5; // bin 16

                    oa->write_character(to_char_type(output_type));
                    write_number(static_cast<std::uint16_t>(N));
                } else if (N <= (std::numeric_limits<std::uint32_t>::max)()) {
                    const std::uint8_t output_type = use_ext
                                                         ? 0xC9  // ext 32
                                                         : 0xC6; // bin 32

                    oa->write_character(to_char_type(output_type));
                    write_number(static_cast<std::uint32_t>(N));
                }

                // step 1.5: if this is an ext type, write the subtype
                if (use_ext) {
                    write_number(static_cast<std::int8_t>(j.m_value.binary->subtype()));
                }

                // step 2: write the byte string
                oa->write_characters(
                    reinterpret_cast<const CharType*>(j.j.get_value()->binary->data()),
                    N);

                break;
            }
            */

            case document::impl::value_type::dict: {
                // step 1: write control byte and the object size
                const auto N = j.get_value()->as_dict()->count();
                if (N <= 15) {
                    // fixmap
                    write_number(static_cast<std::uint8_t>(0x80 | (N & 0xF)));
                } else if (N <= (std::numeric_limits<std::uint16_t>::max)()) {
                    // map 16
                    oa->write_character(to_char_type(0xDE));
                    write_number(static_cast<std::uint16_t>(N));
                } else if (N <= (std::numeric_limits<std::uint32_t>::max)()) {
                    // map 32
                    oa->write_character(to_char_type(0xDF));
                    write_number(static_cast<std::uint32_t>(N));
                }
                for (auto it = j.get_value()->as_dict()->begin(); it; ++it) {

                    write_simple_datatype(it.key());
                    write_simple_datatype(it.value());

                }
                break;
            }

            case document::impl::value_type::undefined:
            default:
                break;
        }
    }

private:

    static constexpr CharType get_msgpack_float_prefix(float /*unused*/) {
        return to_char_type(0xCA); // float 32
    }

    static constexpr CharType get_msgpack_float_prefix(double /*unused*/) {
        return to_char_type(0xCB); // float 64
    }

    template<typename NumberType>
    void write_number(const NumberType n, const bool OutputIsLittleEndian = false) {
        // step 1: write number to array of length NumberType
        std::array<CharType, sizeof(NumberType)> vec{};
        std::memcpy(vec.data(), &n, sizeof(NumberType));

        // step 2: write array to output (with possible reordering)
        if (is_little_endian != OutputIsLittleEndian) {
            // reverse byte order prior to conversion if necessary
            std::reverse(vec.begin(), vec.end());
        }

        oa->write_characters(vec.data(), sizeof(NumberType));
    }

    void write_double(const number_double_t n) {

        oa->write_character(get_msgpack_float_prefix(n));
        write_number(n);
        
    }

public:
    template<typename C = CharType,std::enable_if_t<std::is_signed<C>::value && std::is_signed<char>::value>* = nullptr>
    static constexpr CharType to_char_type(std::uint8_t x) noexcept {
        return *reinterpret_cast<char*>(&x);
    }

    template<typename C = CharType,
             std::enable_if_t<std::is_signed<C>::value && std::is_unsigned<char>::value>* = nullptr>
    static CharType to_char_type(std::uint8_t x) noexcept {
        static_assert(sizeof(std::uint8_t) == sizeof(CharType), "size of CharType must be equal to std::uint8_t");
        static_assert(std::is_trivial<CharType>::value, "CharType must be trivial");
        CharType result;
        std::memcpy(&result, &x, sizeof(x));
        return result;
    }

    template<typename C = CharType,
             std::enable_if_t<std::is_unsigned<C>::value>* = nullptr>
    static constexpr CharType to_char_type(std::uint8_t x) noexcept {
        return x;
    }

    template<typename InputCharType, typename C = CharType,
             std::enable_if_t<
                 std::is_signed<C>::value &&
                 std::is_signed<char>::value &&
                 std::is_same<char, typename std::remove_cv<InputCharType>::type>::value>* = nullptr>
    static constexpr CharType to_char_type(InputCharType x) noexcept {
        return x;
    }

private:
    /// whether we can assume little endianness
    const bool is_little_endian = little_endianness();

    /// the output
    output_adapter_t<CharType> oa = nullptr;
};
