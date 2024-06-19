#pragma once

#include <components/new_document/impl/tape_type.hpp>
#include <cstring>

namespace components::new_document {
    namespace impl {
        template<typename T>
        class base_document;
    } // namespace impl

    namespace internal {

        constexpr uint64_t JSON_VALUE_MASK = 0x00FFFFFFFFFFFFFF;

        template<typename K>
        class tape_ref {
        public:
            tape_ref() noexcept
                : doc_{nullptr}
                , json_index_{0} {}
            tape_ref(const impl::base_document<K>* doc, size_t json_index) noexcept
                : doc_{doc}
                , json_index_{json_index} {}
            tape_type tape_ref_type() const noexcept {
                return static_cast<tape_type>(doc_->get_tape(json_index_) >> 56);
            }
            uint64_t tape_value() const noexcept { return doc_->get_tape(json_index_) & JSON_VALUE_MASK; }

            bool is_float() const noexcept;
            bool is_double() const noexcept;
            bool is_int8() const noexcept;
            bool is_int16() const noexcept;
            bool is_int32() const noexcept;
            bool is_int64() const noexcept;
            bool is_int128() const noexcept;
            bool is_uint8() const noexcept;
            bool is_uint16() const noexcept;
            bool is_uint32() const noexcept;
            bool is_uint64() const noexcept;
            bool is_false() const noexcept;
            bool is_true() const noexcept;
            bool is_null_on_tape() const noexcept; // different name to avoid clash with is_null.

            template<typename T, typename std::enable_if<(sizeof(T) < sizeof(uint64_t)), uint8_t>::type = 0>
            T next_tape_value() const noexcept {
                T x;
                std::memcpy(&x, &doc_->get_tape(json_index_), sizeof(T));
                return x;
            }

            template<typename T, typename std::enable_if<(sizeof(T) >= sizeof(uint64_t)), uint8_t>::type = 1>
            T next_tape_value() const noexcept {
                static_assert(sizeof(T) == sizeof(uint64_t) || sizeof(T) == 2 * sizeof(uint64_t),
                              "next_tape_value() template parameter must be 8, 16, 32, 64 or 128-bit");
                T x;
                std::memcpy(&x, &doc_->get_tape(json_index_ + 1), sizeof(T));
                return x;
            }

            uint32_t get_string_length() const noexcept;
            const char* get_c_str() const noexcept;
            std::string_view get_string_view() const noexcept;
            bool usable() const noexcept {
                return doc_ != nullptr;
            } // when the document pointer is null, this tape_ref is uninitialized (should not be accessed).

            const impl::base_document<K>* doc_;

            size_t json_index_;
        };

        template<typename K>
        bool tape_ref<K>::is_float() const noexcept {
            constexpr auto tape_float = uint8_t(tape_type::FLOAT);
            return reinterpret_cast<const uint8_t*>(&doc_->get_tape(json_index_))[7] == tape_float;
        }
        template<typename K>
        bool tape_ref<K>::is_double() const noexcept {
            constexpr uint64_t tape_double = uint64_t(tape_type::DOUBLE) << 56;
            return doc_->get_tape(json_index_) == tape_double;
        }
        template<typename K>
        bool tape_ref<K>::is_int8() const noexcept {
            constexpr auto tape_int32 = uint8_t(tape_type::INT8);
            return reinterpret_cast<const uint8_t*>(&doc_->get_tape(json_index_))[7] == tape_int32;
        }
        template<typename K>
        bool tape_ref<K>::is_int16() const noexcept {
            constexpr auto tape_int32 = uint8_t(tape_type::INT16);
            return reinterpret_cast<const uint8_t*>(&doc_->get_tape(json_index_))[7] == tape_int32;
        }
        template<typename K>
        bool tape_ref<K>::is_int32() const noexcept {
            constexpr auto tape_int32 = uint8_t(tape_type::INT32);
            return reinterpret_cast<const uint8_t*>(&doc_->get_tape(json_index_))[7] == tape_int32;
        }
        template<typename K>
        bool tape_ref<K>::is_int64() const noexcept {
            constexpr uint64_t tape_int64 = uint64_t(tape_type::INT64) << 56;
            return doc_->get_tape(json_index_) == tape_int64;
        }
        template<typename K>
        bool tape_ref<K>::is_int128() const noexcept {
            constexpr uint64_t tape_int128 = uint64_t(tape_type::INT128) << 56;
            return doc_->get_tape(json_index_) == tape_int128;
        }
        template<typename K>
        bool tape_ref<K>::is_uint8() const noexcept {
            constexpr auto tape_int32 = uint8_t(tape_type::UINT8);
            return reinterpret_cast<const uint8_t*>(&doc_->get_tape(json_index_))[7] == tape_int32;
        }
        template<typename K>
        bool tape_ref<K>::is_uint16() const noexcept {
            constexpr auto tape_int32 = uint8_t(tape_type::UINT16);
            return reinterpret_cast<const uint8_t*>(&doc_->get_tape(json_index_))[7] == tape_int32;
        }
        template<typename K>
        bool tape_ref<K>::is_uint32() const noexcept {
            constexpr auto tape_uint32 = uint8_t(tape_type::UINT32);
            return reinterpret_cast<const uint8_t*>(&doc_->get_tape(json_index_))[7] == tape_uint32;
        }
        template<typename K>
        bool tape_ref<K>::is_uint64() const noexcept {
            constexpr uint64_t tape_uint64 = uint64_t(tape_type::UINT64) << 56;
            return doc_->get_tape(json_index_) == tape_uint64;
        }
        template<typename K>
        bool tape_ref<K>::is_false() const noexcept {
            constexpr uint64_t tape_false = uint64_t(tape_type::FALSE_VALUE) << 56;
            return doc_->get_tape(json_index_) == tape_false;
        }
        template<typename K>
        bool tape_ref<K>::is_true() const noexcept {
            constexpr uint64_t tape_true = uint64_t(tape_type::TRUE_VALUE) << 56;
            return doc_->get_tape(json_index_) == tape_true;
        }
        template<typename K>
        bool tape_ref<K>::is_null_on_tape() const noexcept {
            constexpr uint64_t tape_null = uint64_t(tape_type::NULL_VALUE) << 56;
            return doc_->get_tape(json_index_) == tape_null;
        }

        template<typename K>
        uint32_t tape_ref<K>::get_string_length() const noexcept {
            size_t string_buf_index = size_t(tape_value());
            uint32_t len;
            std::memcpy(&len, &doc_->get_string_buf(string_buf_index), sizeof(len));
            return len;
        }

        template<typename K>
        const char* tape_ref<K>::get_c_str() const noexcept {
            size_t string_buf_index = size_t(tape_value());
            return reinterpret_cast<const char*>(&doc_->get_string_buf(string_buf_index + sizeof(uint32_t)));
        }

        template<typename K>
        std::string_view tape_ref<K>::get_string_view() const noexcept {
            return std::string_view(get_c_str(), get_string_length());
        }

    } // namespace internal
} // namespace components::new_document
