#pragma once

#include <components/document/support/endianness.hpp>
#include <components/document/support/platform_compat.hpp>

namespace document { namespace endian {

#ifdef __BIG_ENDIAN__
    CONST static inline uint64_t enc64(uint64_t v) noexcept { return v; }
    CONST static inline uint64_t dec64(uint64_t v) noexcept { return v; }
    CONST static inline uint32_t enc32(uint32_t v) noexcept { return v; }
    CONST static inline uint32_t dec32(uint32_t v) noexcept { return v; }
    CONST static inline uint16_t enc16(uint16_t v) noexcept { return v; }
    CONST static inline uint16_t dec16(uint16_t v) noexcept { return v; }
#else
    CONST static inline uint64_t enc64(uint64_t v) noexcept { return bswap64(v); }
    CONST static inline uint64_t dec64(uint64_t v) noexcept { return bswap64(v); }
    CONST static inline uint32_t enc32(uint32_t v) noexcept { return bswap32(v); }
    CONST static inline uint32_t dec32(uint32_t v) noexcept { return bswap32(v); }
    CONST static inline uint16_t enc16(uint16_t v) noexcept { return bswap16(v); }
    CONST static inline uint16_t dec16(uint16_t v) noexcept { return bswap16(v); }
#endif

#ifdef __LITTLE_ENDIAN__
    CONST static inline uint64_t little_enc64(uint64_t v) noexcept { return v; }
    CONST static inline uint64_t little_dec64(uint64_t v) noexcept { return v; }
    CONST static inline uint32_t little_enc32(uint32_t v) noexcept { return v; }
    CONST static inline uint32_t little_dec32(uint32_t v) noexcept { return v; }
    CONST static inline uint16_t little_enc16(uint16_t v) noexcept { return v; }
    CONST static inline uint16_t little_dec16(uint16_t v) noexcept { return v; }
#else
    CONST static inline uint64_t little_enc64(uint64_t v) noexcept { return bswap64(v); }
    CONST static inline uint64_t little_dec64(uint64_t v) noexcept { return bswap64(v); }
    CONST static inline uint32_t little_enc32(uint32_t v) noexcept { return bswap32(v); }
    CONST static inline uint32_t little_dec32(uint32_t v) noexcept { return bswap32(v); }
    CONST static inline uint16_t little_enc16(uint16_t v) noexcept { return bswap16(v); }
    CONST static inline uint16_t little_dec16(uint16_t v) noexcept { return bswap16(v); }
#endif

    namespace internal {

        CONST inline uint16_t swap_little(uint16_t n) noexcept { return uint16_t(little_enc16(n)); }
        CONST inline uint16_t swap_big(uint16_t n) noexcept { return uint16_t(enc16(n)); }
        CONST inline uint32_t swap_little(uint32_t n) noexcept { return little_enc32(n); }
        CONST inline uint32_t swap_big(uint32_t n) noexcept { return enc32(n); }
        CONST inline uint64_t swap_little(uint64_t n) noexcept { return little_enc64(n); }
        CONST inline uint64_t swap_big(uint64_t n) noexcept { return enc64(n); }

        inline void swap_little(uint16_t& n) noexcept { n = little_enc16(n); }
        inline void swap_big(uint16_t& n) noexcept { n = uint16_t(enc16(n)); }
        inline void swap_little(uint32_t& n) noexcept { n = little_enc32(n); }
        inline void swap_big(uint32_t& n) noexcept { n = enc32(n); }
        inline void swap_little(uint64_t& n) noexcept { n = little_enc64(n); }
        inline void swap_big(uint64_t& n) noexcept { n = enc64(n); }

        template<class INT, INT SWAP(INT)>
        class endian {
        public:
            endian() noexcept
                : endian(0) {}
            endian(INT o) noexcept
                : _swapped(SWAP(o)) {}
            PURE operator INT() const noexcept { return SWAP(_swapped); }

        private:
            INT _swapped;
        };

        template<class INT, INT SWAP(INT)>
        class endian_unaligned {
        public:
            endian_unaligned() noexcept
                : endian_unaligned(0) {}

            endian_unaligned(INT o) noexcept {
                o = SWAP(o);
                memcpy(_bytes, &o, sizeof(o));
            }

            PURE operator INT() const noexcept {
                INT o = 0;
                memcpy(&o, _bytes, sizeof(o));
                return SWAP(o);
            }

        private:
            uint8_t _bytes[sizeof(INT)];
        };

        template<typename FLT, typename RAW, void SWAP(RAW&)>
        struct endian_float {
            endian_float() noexcept {}
            endian_float(FLT f) noexcept { *this = f; }
            endian_float(RAW raw) noexcept { _swapped.as_raw = raw; }

            endian_float& operator=(FLT f) noexcept {
                _swapped.as_number = f;
                SWAP(_swapped.as_raw);
                return *this;
            }
            PURE operator FLT() const noexcept {
                swapped unswap = _swapped;
                SWAP(unswap.as_raw);
                return unswap.as_number;
            }
            PURE RAW raw() const noexcept { return _swapped.as_raw; }

        protected:
            union swapped {
                FLT as_number;
                RAW as_raw;
            };
            swapped _swapped;
        };

    } // namespace internal

    using uint16_le = internal::endian<uint16_t, internal::swap_little>;
    using uint32_le = internal::endian<uint32_t, internal::swap_little>;
    using uint64_le = internal::endian<uint64_t, internal::swap_little>;

    using uint32_le_unaligned = internal::endian_unaligned<uint32_t, internal::swap_little>;

    using little_float = internal::endian_float<float, uint32_t, internal::swap_little>;
    using big_float = internal::endian_float<float, uint32_t, internal::swap_big>;
    using little_double = internal::endian_float<double, uint64_t, internal::swap_little>;
    using big_double = internal::endian_float<double, uint64_t, internal::swap_big>;

}} // namespace document::endian
