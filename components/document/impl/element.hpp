#pragma once

#include <components/new_document/impl/common_defs.hpp>
#include <components/types/types.hpp>
#include <cstdint>
#include <type_traits>

namespace components::new_document {
    namespace internal {
        template<typename T>
        class tape_ref;
    }
    namespace impl {

        template<typename K,
                 typename From,
                 typename To,
                 typename std::enable_if_t<std::is_signed<From>::value && std::is_signed<To>::value, uint8_t> = 0>
        document_result<To> cast_from(internal::tape_ref<K> tape) noexcept {
            From result = tape.template next_tape_value<From>();
            if (result > From((std::numeric_limits<To>::max)()) || result < From((std::numeric_limits<To>::min)())) {
                return NUMBER_OUT_OF_RANGE;
            }
            return static_cast<To>(result);
        }
        template<typename K,
                 typename From,
                 typename To,
                 typename std::enable_if<std::is_signed<From>::value && std::is_unsigned<To>::value, uint8_t>::type = 1>
        document_result<To> cast_from(internal::tape_ref<K> tape) noexcept {
            From result = tape.template next_tape_value<From>();
            if (result < 0) {
                return NUMBER_OUT_OF_RANGE;
            }
            return static_cast<To>(result);
        }

        template<typename K,
                 typename From,
                 typename To,
                 typename std::enable_if<std::is_unsigned<From>::value, uint8_t>::type = 2>
        document_result<To> cast_from(internal::tape_ref<K> tape) noexcept {
            From result = tape.template next_tape_value<From>();
            // Wrapping max in parens to handle Windows issue: https://stackoverflow.com/questions/11544073/how-do-i-deal-with-the-max-macro-in-windows-h-colliding-with-max-in-std
            if (result > From((std::numeric_limits<To>::max)())) {
                return NUMBER_OUT_OF_RANGE;
            }
            return static_cast<To>(result);
        }

        // specifications for __int128_t:
        template<
            typename K,
            typename From,
            typename To,
            typename std::enable_if_t<std::is_same<From, __int128_t>::value && std::is_signed<To>::value, uint8_t> = 0>
        document_result<To> cast_from(internal::tape_ref<K> tape) noexcept {
            From result = tape.template next_tape_value<From>();
            if (result > From((std::numeric_limits<To>::max)()) || result < From((std::numeric_limits<To>::min)())) {
                return NUMBER_OUT_OF_RANGE;
            }
            return static_cast<To>(result);
        }

        template<
            typename K,
            typename From,
            typename To,
            typename std::enable_if_t<std::is_signed<From>::value && std::is_same<To, __int128_t>::value, uint8_t> = 0>
        document_result<To> cast_from(internal::tape_ref<K> tape) noexcept {
            From result = tape.template next_tape_value<From>();
            if (result > From((std::numeric_limits<To>::max)()) || result < From((std::numeric_limits<To>::min)())) {
                return NUMBER_OUT_OF_RANGE;
            }
            return static_cast<To>(result);
        }
        template<typename K,
                 typename From,
                 typename To,
                 typename std::enable_if<std::is_same<From, __int128_t>::value && std::is_unsigned<To>::value,
                                         uint8_t>::type = 1>
        document_result<To> cast_from(internal::tape_ref<K> tape) noexcept {
            From result = tape.template next_tape_value<From>();
            if (result < 0) {
                return NUMBER_OUT_OF_RANGE;
            }
            return static_cast<To>(result);
        }

        template<typename K>
        class element {
        public:
            element() noexcept
                : tape() {}

            types::logical_type logical_type() const noexcept {
                assert(tape.usable());
                return types::to_logical(tape.tape_ref_type());
            }

            types::physical_type physical_type() const noexcept {
                assert(tape.usable());
                return tape.tape_ref_type();
            }

            std::pmr::string serialize() const noexcept { return tape.serialize(); }

            // Cast this element to a null-terminated C string.
            document_result<const char*> get_c_str() const noexcept {
                assert(tape.usable());
                switch (tape.tape_ref_type()) {
                    case types::physical_type::STRING: {
                        return tape.get_c_str();
                    }
                    default:
                        return INCORRECT_TYPE;
                }
            }
            document_result<size_t> get_string_length() const noexcept {
                assert(tape.usable());
                switch (tape.tape_ref_type()) {
                    case types::physical_type::STRING: {
                        return tape.get_string_length();
                    }
                    default:
                        return INCORRECT_TYPE;
                }
            }
            document_result<std::string_view> get_string() const noexcept {
                assert(tape.usable());
                switch (tape.tape_ref_type()) {
                    case types::physical_type::STRING:
                        return tape.get_string_view();
                    default:
                        return INCORRECT_TYPE;
                }
            }

            document_result<uint8_t> get_uint8() const noexcept {
                assert(tape.usable());
                if (_usually_false(!tape.is_uint8())) { // branch rarely taken
                    switch (tape.tape_ref_type()) {
                        case types::physical_type::INT8: {
                            return cast_from<K, int8_t, uint8_t>(tape);
                        }
                        case types::physical_type::UINT16: {
                            return cast_from<K, uint16_t, uint8_t>(tape);
                        }
                        case types::physical_type::INT16: {
                            return cast_from<K, int16_t, uint8_t>(tape);
                        }
                        case types::physical_type::UINT32: {
                            return cast_from<K, uint32_t, uint8_t>(tape);
                        }
                        case types::physical_type::INT32: {
                            return cast_from<K, int32_t, uint8_t>(tape);
                        }
                        case types::physical_type::UINT64: {
                            return cast_from<K, uint64_t, uint8_t>(tape);
                        }
                        case types::physical_type::INT64: {
                            return cast_from<K, int64_t, uint8_t>(tape);
                        }
                        case types::physical_type::INT128: {
                            return cast_from<K, __int128_t, uint8_t>(tape);
                        }
                        default:
                            return INCORRECT_TYPE;
                    }
                }
                return tape.template next_tape_value<uint8_t>();
            }

            document_result<uint16_t> get_uint16() const noexcept {
                assert(tape.usable());
                if (_usually_false(!tape.is_uint16())) { // branch rarely taken
                    switch (tape.tape_ref_type()) {
                        case types::physical_type::UINT8: {
                            return uint16_t(tape.template next_tape_value<uint8_t>());
                        }
                        case types::physical_type::INT8: {
                            return uint16_t(tape.template next_tape_value<int8_t>());
                        }
                        case types::physical_type::INT16: {
                            return cast_from<K, int16_t, uint16_t>(tape);
                        }
                        case types::physical_type::UINT32: {
                            return cast_from<K, uint32_t, uint16_t>(tape);
                        }
                        case types::physical_type::INT32: {
                            return cast_from<K, int32_t, uint16_t>(tape);
                        }
                        case types::physical_type::UINT64: {
                            return cast_from<K, uint64_t, uint16_t>(tape);
                        }
                        case types::physical_type::INT64: {
                            return cast_from<K, int64_t, uint16_t>(tape);
                        }
                        case types::physical_type::INT128: {
                            return cast_from<K, __int128_t, uint16_t>(tape);
                        }
                        default:
                            return INCORRECT_TYPE;
                    }
                }
                return tape.template next_tape_value<uint16_t>();
            }

            document_result<uint32_t> get_uint32() const noexcept {
                assert(tape.usable());
                if (_usually_false(!tape.is_uint32())) { // branch rarely taken
                    switch (tape.tape_ref_type()) {
                        case types::physical_type::UINT8: {
                            return uint32_t(tape.template next_tape_value<uint8_t>());
                        }
                        case types::physical_type::INT8: {
                            return uint32_t(tape.template next_tape_value<int8_t>());
                        }
                        case types::physical_type::UINT16: {
                            return uint32_t(tape.template next_tape_value<uint16_t>());
                        }
                        case types::physical_type::INT16: {
                            return cast_from<K, int16_t, uint32_t>(tape);
                        }
                        case types::physical_type::INT32: {
                            return cast_from<K, int32_t, uint32_t>(tape);
                        }
                        case types::physical_type::UINT64: {
                            return cast_from<K, uint64_t, uint32_t>(tape);
                        }
                        case types::physical_type::INT64: {
                            return cast_from<K, int64_t, uint32_t>(tape);
                        }
                        case types::physical_type::INT128: {
                            return cast_from<K, __int128_t, uint32_t>(tape);
                        }
                        default:
                            return INCORRECT_TYPE;
                    }
                }
                return tape.template next_tape_value<uint32_t>();
            }

            document_result<uint64_t> get_uint64() const noexcept {
                assert(tape.usable());
                if (_usually_false(!tape.is_uint64())) { // branch rarely taken
                    switch (tape.tape_ref_type()) {
                        case types::physical_type::UINT8: {
                            return uint64_t(tape.template next_tape_value<uint8_t>());
                        }
                        case types::physical_type::INT8: {
                            return uint64_t(tape.template next_tape_value<int8_t>());
                        }
                        case types::physical_type::UINT16: {
                            return uint64_t(tape.template next_tape_value<uint16_t>());
                        }
                        case types::physical_type::UINT32: {
                            return uint64_t(tape.template next_tape_value<uint32_t>());
                        }
                        case types::physical_type::INT16: {
                            return cast_from<K, int16_t, uint64_t>(tape);
                        }
                        case types::physical_type::INT32: {
                            return cast_from<K, int32_t, uint64_t>(tape);
                        }
                        case types::physical_type::INT64: {
                            return cast_from<K, int64_t, uint64_t>(tape);
                        }
                        case types::physical_type::INT128: {
                            return cast_from<K, __int128_t, uint64_t>(tape);
                        }
                        default:
                            return INCORRECT_TYPE;
                    }
                }
                return tape.template next_tape_value<uint64_t>();
            }

            document_result<int8_t> get_int8() const noexcept {
                assert(tape.usable());
                if (_usually_false(!tape.is_int8())) { // branch rarely taken
                    switch (tape.tape_ref_type()) {
                        case types::physical_type::UINT8: {
                            return int8_t(tape.template next_tape_value<uint8_t>());
                        }
                        case types::physical_type::INT16: {
                            return cast_from<K, int16_t, int8_t>(tape);
                        }
                        case types::physical_type::UINT16: {
                            return cast_from<K, uint16_t, int8_t>(tape);
                        }
                        case types::physical_type::INT32: {
                            return cast_from<K, int32_t, int8_t>(tape);
                        }
                        case types::physical_type::UINT32: {
                            return cast_from<K, uint32_t, int8_t>(tape);
                        }
                        case types::physical_type::INT64: {
                            return cast_from<K, int64_t, int8_t>(tape);
                        }
                        case types::physical_type::UINT64: {
                            return cast_from<K, uint64_t, int8_t>(tape);
                        }
                        case types::physical_type::INT128: {
                            return cast_from<K, __int128_t, int8_t>(tape);
                        }
                        default:
                            return INCORRECT_TYPE;
                    }
                }
                return tape.template next_tape_value<int8_t>();
            }

            document_result<int16_t> get_int16() const noexcept {
                assert(tape.usable());
                if (_usually_false(!tape.is_int16())) { // branch rarely taken
                    switch (tape.tape_ref_type()) {
                        case types::physical_type::INT8: {
                            return int16_t(tape.template next_tape_value<int8_t>());
                        }
                        case types::physical_type::UINT8: {
                            return int16_t(tape.template next_tape_value<uint8_t>());
                        }
                        case types::physical_type::UINT16: {
                            return cast_from<K, uint16_t, int16_t>(tape);
                        }
                        case types::physical_type::INT32: {
                            return cast_from<K, int32_t, int16_t>(tape);
                        }
                        case types::physical_type::UINT32: {
                            return cast_from<K, uint32_t, int16_t>(tape);
                        }
                        case types::physical_type::INT64: {
                            return cast_from<K, int64_t, int16_t>(tape);
                        }
                        case types::physical_type::UINT64: {
                            return cast_from<K, uint64_t, int16_t>(tape);
                        }
                        case types::physical_type::INT128: {
                            return cast_from<K, __int128_t, int16_t>(tape);
                        }
                        default:
                            return INCORRECT_TYPE;
                    }
                }
                return tape.template next_tape_value<int16_t>();
            }

            document_result<int32_t> get_int32() const noexcept {
                assert(tape.usable());
                if (_usually_false(!tape.is_int32())) { // branch rarely taken
                    switch (tape.tape_ref_type()) {
                        case types::physical_type::INT8: {
                            return int32_t(tape.template next_tape_value<int8_t>());
                        }
                        case types::physical_type::UINT8: {
                            return int32_t(tape.template next_tape_value<uint8_t>());
                        }
                        case types::physical_type::INT16: {
                            return int32_t(tape.template next_tape_value<int16_t>());
                        }
                        case types::physical_type::UINT16: {
                            return int32_t(tape.template next_tape_value<uint16_t>());
                        }
                        case types::physical_type::UINT32: {
                            return cast_from<K, uint32_t, int32_t>(tape);
                        }
                        case types::physical_type::INT64: {
                            return cast_from<K, int64_t, int32_t>(tape);
                        }
                        case types::physical_type::UINT64: {
                            return cast_from<K, uint64_t, int32_t>(tape);
                        }
                        case types::physical_type::INT128: {
                            return cast_from<K, __int128_t, int32_t>(tape);
                        }
                        default:
                            return INCORRECT_TYPE;
                    }
                }
                return tape.template next_tape_value<int32_t>();
            }

            document_result<int64_t> get_int64() const noexcept {
                assert(tape.usable());
                if (_usually_false(!tape.is_int64())) { // branch rarely taken
                    switch (tape.tape_ref_type()) {
                        case types::physical_type::INT8: {
                            return int64_t(tape.template next_tape_value<int8_t>());
                        }
                        case types::physical_type::UINT8: {
                            return int64_t(tape.template next_tape_value<uint8_t>());
                        }
                        case types::physical_type::INT16: {
                            return int32_t(tape.template next_tape_value<int16_t>());
                        }
                        case types::physical_type::UINT16: {
                            return int64_t(tape.template next_tape_value<uint16_t>());
                        }
                        case types::physical_type::INT32: {
                            return int64_t(tape.template next_tape_value<int32_t>());
                        }
                        case types::physical_type::UINT32: {
                            return int64_t(tape.template next_tape_value<uint32_t>());
                        }
                        case types::physical_type::UINT64: {
                            return cast_from<K, uint64_t, int64_t>(tape);
                        }
                        case types::physical_type::INT128: {
                            return cast_from<K, __int128_t, int64_t>(tape);
                        }
                        default:
                            return INCORRECT_TYPE;
                    }
                }
                return tape.template next_tape_value<int64_t>();
            }

            document_result<__int128_t> get_int128() const noexcept {
                assert(tape.usable());
                if (_usually_false(!tape.is_int128())) { // branch rarely taken
                    switch (tape.tape_ref_type()) {
                        case types::physical_type::INT8: {
                            return __int128_t(tape.template next_tape_value<int8_t>());
                        }
                        case types::physical_type::UINT8: {
                            return __int128_t(tape.template next_tape_value<uint8_t>());
                        }
                        case types::physical_type::INT16: {
                            return __int128_t(tape.template next_tape_value<int16_t>());
                        }
                        case types::physical_type::UINT16: {
                            return __int128_t(tape.template next_tape_value<uint16_t>());
                        }
                        case types::physical_type::INT32: {
                            return __int128_t(tape.template next_tape_value<int32_t>());
                        }
                        case types::physical_type::UINT32: {
                            return __int128_t(tape.template next_tape_value<uint32_t>());
                        }
                        case types::physical_type::INT64: {
                            return cast_from<K, int64_t, __int128_t>(tape);
                        }
                        case types::physical_type::UINT64: {
                            return cast_from<K, uint64_t, __int128_t>(tape);
                        }
                        default:
                            return INCORRECT_TYPE;
                    }
                }
                return tape.template next_tape_value<__int128_t>();
            }

            document_result<float> get_float() const noexcept {
                assert(tape.usable());
                if (_usually_false(!tape.is_float())) { // branch rarely taken
                    switch (tape.tape_ref_type()) {
                        case types::physical_type::DOUBLE: {
                            return float(tape.template next_tape_value<double>());
                        }
                        case types::physical_type::UINT8: {
                            return float(tape.template next_tape_value<uint8_t>());
                        }
                        case types::physical_type::INT8: {
                            return float(tape.template next_tape_value<int8_t>());
                        }
                        case types::physical_type::UINT16: {
                            return float(tape.template next_tape_value<uint16_t>());
                        }
                        case types::physical_type::INT16: {
                            return float(tape.template next_tape_value<int16_t>());
                        }
                        case types::physical_type::UINT32: {
                            return float(tape.template next_tape_value<uint32_t>());
                        }
                        case types::physical_type::INT32: {
                            return float(tape.template next_tape_value<int32_t>());
                        }
                        case types::physical_type::UINT64: {
                            return float(tape.template next_tape_value<uint64_t>());
                        }
                        case types::physical_type::INT64: {
                            return float(tape.template next_tape_value<int64_t>());
                        }
                        case types::physical_type::INT128: {
                            return float(tape.template next_tape_value<__int128_t>());
                        }
                        default:
                            return INCORRECT_TYPE;
                    }
                }
                // this is common:
                return tape.template next_tape_value<float>();
            }

            document_result<double> get_double() const noexcept {
                assert(tape.usable());
                if (_usually_false(!tape.is_double())) { // branch rarely taken    switch (tape.tape_ref_type()) {
                    switch (tape.tape_ref_type()) {
                        case types::physical_type::FLOAT: {
                            return double(tape.template next_tape_value<float>());
                        }
                        case types::physical_type::UINT8: {
                            return double(tape.template next_tape_value<uint8_t>());
                        }
                        case types::physical_type::INT8: {
                            return double(tape.template next_tape_value<int8_t>());
                        }
                        case types::physical_type::UINT16: {
                            return double(tape.template next_tape_value<uint16_t>());
                        }
                        case types::physical_type::INT16: {
                            return double(tape.template next_tape_value<int16_t>());
                        }
                        case types::physical_type::UINT32: {
                            return double(tape.template next_tape_value<uint32_t>());
                        }
                        case types::physical_type::INT32: {
                            return double(tape.template next_tape_value<int32_t>());
                        }
                        case types::physical_type::UINT64: {
                            return double(tape.template next_tape_value<uint64_t>());
                        }
                        case types::physical_type::INT64: {
                            return double(tape.template next_tape_value<int64_t>());
                        }
                        case types::physical_type::INT128: {
                            return double(tape.template next_tape_value<__int128_t>());
                        }
                        default:
                            return INCORRECT_TYPE;
                    }
                }
                return tape.template next_tape_value<double>();
            }

            document_result<bool> get_bool() const noexcept {
                assert(tape.usable());
                if (tape.is_true()) {
                    return true;
                } else if (tape.is_false()) {
                    return false;
                }
                return INCORRECT_TYPE;
            }

            bool is_string() const noexcept { return is<std::string_view>(); }
            bool is_int32() const noexcept { return is<int32_t>(); }
            bool is_int64() const noexcept { return is<int64_t>(); }
            bool is_int128() const noexcept { return is<__int128_t>(); }
            bool is_uint32() const noexcept { return is<uint32_t>(); }
            bool is_uint64() const noexcept { return is<uint64_t>(); }
            bool is_float() const noexcept { return is<float>(); }
            bool is_double() const noexcept { return is<double>(); }
            bool is_bool() const noexcept { return is<bool>(); }
            bool is_number() const noexcept { return is_int64() || is_uint64() || is_double(); }

            bool is_null() const noexcept { return tape.is_null_on_tape(); }

            template<typename T>
            bool is() const noexcept {
                auto result = get<T>();
                return !result.error();
            }

            template<typename T>
            typename std::enable_if<T::value, document_result<T>>::type get() const noexcept {
                // calling this method should
                // immediately fail.
                static_assert(!sizeof(T),
                              "The get method with given type is not implemented. "
                              "The supported types are Boolean (bool), numbers (double, uint64_t, int64_t), "
                              "strings (std::string_view, const char *). "
                              "We recommend you use get_double(), get_bool(), get_uint64(), get_int64(), "
                              "or get_string() instead of the get template.");
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, const char*>::value, document_result<T>>::type
            get() const noexcept {
                return get_c_str();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, std::string_view>::value, document_result<T>>::type
            get() const noexcept {
                return get_string();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, int8_t>::value, document_result<T>>::type get() const noexcept {
                return get_int8();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, int16_t>::value, document_result<T>>::type get() const noexcept {
                return get_int16();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, int32_t>::value, document_result<T>>::type get() const noexcept {
                return get_int32();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, int64_t>::value, document_result<T>>::type get() const noexcept {
                return get_int64();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, __int128_t>::value, document_result<T>>::type get() const noexcept {
                return get_int128();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, uint8_t>::value, document_result<T>>::type get() const noexcept {
                return get_uint8();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, uint16_t>::value, document_result<T>>::type get() const noexcept {
                return get_uint16();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, uint32_t>::value, document_result<T>>::type get() const noexcept {
                return get_uint32();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, uint64_t>::value, document_result<T>>::type get() const noexcept {
                return get_uint64();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, float>::value, document_result<T>>::type get() const noexcept {
                return get_float();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, double>::value, document_result<T>>::type get() const noexcept {
                return get_double();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, bool>::value, document_result<T>>::type get() const noexcept {
                return get_bool();
            }

            template<typename T>
            error_code get(T& value) const noexcept {
                return get<T>().get(value);
            }

            // An element-specific version prevents recursion with document_result::get<element>(value)
            template<typename T>
            typename std::enable_if<std::is_same<T, element<K>>::value, document_result<T>>::type
            get(element& value) const noexcept {
                value = element(tape);
                return SUCCESS;
            }

            template<typename T>
            void tie(T& value, error_code& error) && noexcept {
                error = get<T>(value);
            }

            bool operator<(const element& other) const noexcept { return tape.json_index < other.tape.json_index; }

            bool operator==(const element& other) const noexcept { return tape.json_index == other.tape.json_index; }

        private:
            element(const internal::tape_ref<K>& tape) noexcept
                : tape{tape} {}
            internal::tape_ref<K> tape;

            template<typename T>
            friend class base_document;
            friend struct document_result<element>;
        };

        inline std::ostream& operator<<(std::ostream& out, types::logical_type type) {
            switch (type) {
                case types::logical_type::HUGEINT:
                    return out << "int64_t";
                case types::logical_type::UHUGEINT:
                    return out << "uint64_t";
                case types::logical_type::DOUBLE:
                    return out << "double";
                case types::logical_type::STRING_LITERAL:
                    return out << "string";
                case types::logical_type::BOOLEAN:
                    return out << "bool";
                case types::logical_type::NA:
                    return out << "null";
                default:
                    return out << "unexpected content!!!"; // abort() usage is forbidden in the library
            }
        }

    } // namespace impl
} // namespace components::new_document
