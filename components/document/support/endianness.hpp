#pragma once

#include <stdlib.h>
#include <stdint.h>

#ifdef __has_include
#  if __has_include(<endian.h>)
#    include <endian.h>
#  elif __has_include(<machine/endian.h>)
#    include <machine/endian.h>
#  elif __has_include(<sys/param.h>)
#    include <sys/param.h>
#  elif __has_include(<sys/isadefs.h>)
#    include <sys/isadefs.h>
#  endif
#endif

#if !defined(__LITTLE_ENDIAN__) && !defined(__BIG_ENDIAN__)
#  if (defined(__BYTE_ORDER__)  && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__) || \
     (defined(__BYTE_ORDER) && __BYTE_ORDER == __BIG_ENDIAN) || \
	 (defined(_BYTE_ORDER) && _BYTE_ORDER == _BIG_ENDIAN) || \
	 (defined(BYTE_ORDER) && BYTE_ORDER == BIG_ENDIAN) || \
     (defined(__sun) && defined(__SVR4) && defined(_BIG_ENDIAN)) || \
     defined(__ARMEB__) || defined(__THUMBEB__) || defined(__AARCH64EB__) || \
     defined(_MIBSEB) || defined(__MIBSEB) || defined(__MIBSEB__) || \
     defined(_M_PPC)
#    define __BIG_ENDIAN__
#  elif (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) || \
     (defined(__BYTE_ORDER) && __BYTE_ORDER == __LITTLE_ENDIAN)  || \
	 (defined(_BYTE_ORDER) && _BYTE_ORDER == _LITTLE_ENDIAN) || \
	 (defined(BYTE_ORDER) && BYTE_ORDER == LITTLE_ENDIAN)  ||  \
     (defined(__sun) && defined(__SVR4) && defined(_LITTLE_ENDIAN)) ||  \
     defined(__ARMEL__) || defined(__THUMBEL__) || defined(__AARCH64EL__) || \
     defined(_MIPSEL) || defined(__MIPSEL) || defined(__MIPSEL__) || \
     defined(_M_IX86) || defined(_M_X64) || defined(_M_IA64) ||  \
     defined(_M_ARM) ||  defined(_M_ARM64)
#    define __LITTLE_ENDIAN__
#  endif
#endif

#if defined(bswap16) || defined(bswap32) || defined(bswap64)
#  error "unexpected define!"
#endif

#if defined(_MSC_VER)
#  define bswap16(x)     _byteswap_ushort((x))
#  define bswap32(x)     _byteswap_ulong((x))
#  define bswap64(x)     _byteswap_uint64((x))
#elif (__GNUC__ > 4) || (__GNUC__ == 4 && __GNUC_MINOR__ >= 8)
#  define bswap16(x)     __builtin_bswap16((x))
#  define bswap32(x)     __builtin_bswap32((x))
#  define bswap64(x)     __builtin_bswap64((x))
#elif defined(__has_builtin) && __has_builtin(__builtin_bswap64)
#  define bswap16(x)     __builtin_bswap16((x))
#  define bswap32(x)     __builtin_bswap32((x))
#  define bswap64(x)     __builtin_bswap64((x))
#else
    static inline uint16_t bswap16(uint16_t x) {
		return ((( x  >> 8 ) & 0xffu ) | (( x  & 0xffu ) << 8 ));
	}
    static inline uint32_t bswap32(uint32_t x) {
        return ((( x & 0xff000000u ) >> 24 ) |
                (( x & 0x00ff0000u ) >> 8  ) |
                (( x & 0x0000ff00u ) << 8  ) |
                (( x & 0x000000ffu ) << 24 ));
    }
    static inline uint64_t bswap64(uint64_t x) {
        return ((( x & 0xff00000000000000ull ) >> 56 ) |
                (( x & 0x00ff000000000000ull ) >> 40 ) |
                (( x & 0x0000ff0000000000ull ) >> 24 ) |
                (( x & 0x000000ff00000000ull ) >> 8  ) |
                (( x & 0x00000000ff000000ull ) << 8  ) |
                (( x & 0x0000000000ff0000ull ) << 24 ) |
                (( x & 0x000000000000ff00ull ) << 40 ) |
                (( x & 0x00000000000000ffull ) << 56 ));
    }
#endif

#if defined(__LITTLE_ENDIAN__)
#  define ntoh16(x)     bswap16((x))
#  define hton16(x)     bswap16((x))
#  define ntoh32(x)     bswap32((x))
#  define hton32(x)     bswap32((x))
#  define ntoh64(x)     bswap64((x))
#  define hton64(x)     bswap64((x))
#elif defined(__BIG_ENDIAN__)
#  define ntoh16(x)     (x)
#  define hton16(x)     (x)
#  define ntoh32(x)     (x)
#  define hton32(x)     (x)
#  define ntoh64(x)     (x)
#  define hton64(x)     (x)
#  else
#   pragma message ("warning: UNKNOWN Platform / endianness; network / host byte swaps not defined.")
#endif
