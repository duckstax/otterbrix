#include "swift_dtoa.hpp"
#include <float.h>
#include <inttypes.h>
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

namespace storage {

static int binaryExponentFor10ToThe(int p);
static int decimalExponentFor2ToThe(int e);
static uint64_t multiply64x32RoundingDown(uint64_t lhs, uint32_t rhs);
static uint64_t multiply64x32RoundingUp(uint64_t lhs, uint32_t rhs);
static uint64_t multiply64x64RoundingDown(uint64_t lhs, uint64_t rhs);
static uint64_t multiply64x64RoundingUp(uint64_t lhs, uint64_t rhs);
static void intervalContainingPowerOf10_Float(int p, uint64_t *lower, uint64_t *upper, int *exponent);

typedef __uint128_t swift_uint128_t;

#define initialize128WithHighLow64(dest, high64, low64) ((dest) = ((__uint128_t)(high64) << 64) | (low64))
#define increment128(dest) ((dest) += 1)
#define isLessThan128x128(lhs, rhs) ((lhs) < (rhs))
#define subtract128x128(lhs, rhs) (*(lhs) -= (rhs))
#define multiply128xi32(lhs, rhs) (*(lhs) *= (rhs))
#define initialize128WithHigh64(dest, value) ((dest) = (__uint128_t)(value) << 64)
#define extractHigh64From128(arg) ((uint64_t)((arg) >> 64))

#define initialize192WithHighMidLow64(dest, high64, mid64, low64) \
    ((dest).low = (low64),                                        \
     (dest).mid = (mid64),                                        \
     (dest).high = (high64))

static int extractIntegerPart128(__uint128_t *fixed128, int fractionBits) {
    return (int)(*fixed128 >> fractionBits);
}
static void clearIntegerPart128(__uint128_t *fixed128, int fractionBits) {
    const swift_uint128_t fixedPointMask = (((__uint128_t)1 << fractionBits) - 1);
    *fixed128 &= fixedPointMask;
}
static swift_uint128_t multiply128x64RoundingDown(swift_uint128_t lhs, uint64_t rhs);
static swift_uint128_t multiply128x64RoundingUp(swift_uint128_t lhs, uint64_t rhs);
static swift_uint128_t shiftRightRoundingDown128(swift_uint128_t lhs, int shift);
static swift_uint128_t shiftRightRoundingUp128(swift_uint128_t lhs, int shift);
static void intervalContainingPowerOf10_Double(int p, swift_uint128_t *lower, swift_uint128_t *upper, int *exponent);

typedef struct {uint64_t low, mid, high;} swift_uint192_t;

static void multiply192x64RoundingDown(swift_uint192_t *lhs, uint64_t rhs);
static void multiply192x64RoundingUp(swift_uint192_t *lhs, uint64_t rhs);
static void multiply192xi32(swift_uint192_t *lhs, uint32_t rhs);
static void multiply192x128RoundingDown(swift_uint192_t *lhs, swift_uint128_t rhs);
static void multiply192x128RoundingUp(swift_uint192_t *lhs, swift_uint128_t rhs);
static void subtract192x192(swift_uint192_t *lhs, swift_uint192_t rhs);
static int isLessThan192x192(swift_uint192_t lhs, swift_uint192_t rhs);
static void shiftRightRoundingDown192(swift_uint192_t *lhs, int shift);
static void shiftRightRoundingUp192(swift_uint192_t *lhs, int shift);
static void intervalContainingPowerOf10_Float80(int p, swift_uint192_t *lower, swift_uint192_t *upper, int *exponent);

static uint64_t bitPatternForDouble(double d) {
    union { double d; uint64_t u; } converter;
    converter.d = d;
    return converter.u;
}

int swift_decompose_double(double d, int8_t *digits, size_t digits_length, int *decimalExponent) {
    static const int significandBitCount = DBL_MANT_DIG - 1;
    static const uint64_t significandMask
        = ((uint64_t)1 << significandBitCount) - 1;
    static const int exponentBitCount = 11;
    static const int exponentMask = (1 << exponentBitCount) - 1;
    static const int64_t exponentBias = (1 << (exponentBitCount - 1)) - 2;

    uint64_t raw = bitPatternForDouble(d);
    int exponentBitPattern = (raw >> significandBitCount) & exponentMask;
    uint64_t significandBitPattern = raw & significandMask;

    int binaryExponent;
    uint64_t significand;
    if (digits_length < 17) {
        return 0;
    } else if (exponentBitPattern == exponentMask) {
        return 0;
    } else if (exponentBitPattern == 0) {
        if (significandBitPattern == 0) {
            digits[0] = 0;
            *decimalExponent = 0;
            return 1;
        } else {
            binaryExponent = 1 - exponentBias;
            significand = significandBitPattern
                          << (64 - significandBitCount - 1);
        }
    } else {
        binaryExponent = exponentBitPattern - exponentBias;
        uint64_t hiddenBit = (uint64_t)1 << significandBitCount;
        uint64_t fullSignificand = significandBitPattern + hiddenBit;
        significand = fullSignificand << (64 - significandBitCount - 1);
    }

    uint64_t halfUlp = (uint64_t)1 << (64 - significandBitCount - 2);
    uint64_t quarterUlp = halfUlp >> 1;
    uint64_t upperMidpointExact = significand + halfUlp;

    int isBoundary = significandBitPattern == 0;
    uint64_t lowerMidpointExact
        = significand - (isBoundary ? quarterUlp : halfUlp);
    int base10Exponent = decimalExponentFor2ToThe(binaryExponent);

    swift_uint128_t powerOfTenRoundedDown;
    swift_uint128_t powerOfTenRoundedUp;
    int powerOfTenExponent = 0;
    intervalContainingPowerOf10_Double(-base10Exponent,
                                       &powerOfTenRoundedDown,
                                       &powerOfTenRoundedUp,
                                       &powerOfTenExponent);
    const int extraBits = binaryExponent + powerOfTenExponent;

    static const int integerBits = 14;
    static const int fractionBits = 128 - integerBits;

    swift_uint128_t u, l;
    if (significandBitPattern & 1) {
        swift_uint128_t u1 = multiply128x64RoundingDown(powerOfTenRoundedDown, upperMidpointExact);
        u = shiftRightRoundingDown128(u1, integerBits - extraBits);
        swift_uint128_t l1 = multiply128x64RoundingUp(powerOfTenRoundedUp, lowerMidpointExact);
        l = shiftRightRoundingUp128(l1, integerBits - extraBits);
    } else {
        swift_uint128_t u1 = multiply128x64RoundingUp(powerOfTenRoundedUp, upperMidpointExact);
        u = shiftRightRoundingUp128(u1, integerBits - extraBits);
        swift_uint128_t l1 = multiply128x64RoundingDown(powerOfTenRoundedDown, lowerMidpointExact);
        l = shiftRightRoundingDown128(l1, integerBits - extraBits);
    }
    swift_uint128_t t = u;
    swift_uint128_t delta = u;
    subtract128x128(&delta, l);
    int exponent = base10Exponent + 1;

    static const swift_uint128_t fixedPointOne = (__uint128_t)1 << fractionBits;
    while (t < fixedPointOne) {
        exponent -= 1;
        multiply128xi32(&delta, 10);
        multiply128xi32(&t, 10);
    }
    int8_t *digit_p = digits;
    int nextDigit = extractIntegerPart128(&t, fractionBits);
    clearIntegerPart128(&t, fractionBits);
    swift_uint128_t d0 = delta;
    multiply128xi32(&d0, 10000);
    swift_uint128_t t0 = t;
    multiply128xi32(&t0, 10000);
    int fourDigits = extractIntegerPart128(&t0, fractionBits);
    clearIntegerPart128(&t0, fractionBits);
    while (isLessThan128x128(d0, t0)) {
        *digit_p++ = nextDigit;
        int d2 = fourDigits / 100;
        *digit_p++ = d2 / 10;
        *digit_p++ = d2 % 10;
        d2 = fourDigits % 100;
        *digit_p++ = d2 / 10;
        nextDigit = d2 % 10;
        t = t0;
        delta = d0;
        multiply128xi32(&d0, 10000);
        multiply128xi32(&t0, 10000);
        fourDigits = extractIntegerPart128(&t0, fractionBits);
        clearIntegerPart128(&t0, fractionBits);
    }
    while (isLessThan128x128(delta, t)) {
        *digit_p++ = nextDigit;
        multiply128xi32(&delta, 10);
        multiply128xi32(&t, 10);
        nextDigit = extractIntegerPart128(&t, fractionBits);
        clearIntegerPart128(&t, fractionBits);
    }
    uint64_t deltaHigh64 = extractHigh64From128(delta);
    uint64_t tHigh64 = extractHigh64From128(t);
    if (deltaHigh64 > tHigh64 + ((uint64_t)1 << (fractionBits % 64))) {
        uint64_t skew;
        if (isBoundary) {
            skew = deltaHigh64 - deltaHigh64 / 3 - tHigh64;
        } else {
            skew = deltaHigh64 / 2 - tHigh64;
        }
        uint64_t one = (uint64_t)(1) << (64 - integerBits);
        uint64_t fractionMask = one - 1;
        uint64_t oneHalf = one >> 1;
        if ((skew & fractionMask) == oneHalf) {
            int adjust = (int)(skew >> (64 - integerBits));
            nextDigit = (nextDigit - adjust) & ~1;
        } else {
            int adjust = (int)((skew + oneHalf) >> (64 - integerBits));
            nextDigit = (nextDigit - adjust);
        }
    }
    *digit_p++ = nextDigit;

    *decimalExponent = exponent;
    return digit_p - digits;
}

static uint64_t bitPatternForFloat(float f) {
    union { float f; uint32_t u; } converter;
    converter.f = f;
    return converter.u;
}

int swift_decompose_float(float f, int8_t *digits, size_t digits_length, int *decimalExponent) {
    static const int significandBitCount = FLT_MANT_DIG - 1;
    static const uint32_t significandMask = ((uint32_t)1 << significandBitCount) - 1;
    static const int exponentBitCount = 8;
    static const int exponentMask = (1 << exponentBitCount) - 1;
    static const int64_t exponentBias = (1 << (exponentBitCount - 1)) - 2;

    uint32_t raw = bitPatternForFloat(f);
    int exponentBitPattern = (raw >> significandBitCount) & exponentMask;
    uint32_t significandBitPattern = raw & significandMask;

    int binaryExponent;
    uint32_t significand;
    if (digits_length < 9) {
        return 0;
    } else if (exponentBitPattern == exponentMask) {
        return 0;
    } else if (exponentBitPattern == 0) {
        if (significandBitPattern == 0) {
            digits[0] = 0;
            *decimalExponent = 0;
            return 1;
        } else {
            binaryExponent = 1 - exponentBias;
            significand = significandBitPattern << (32 - significandBitCount - 1);
        }
    } else {
        binaryExponent = exponentBitPattern - exponentBias;
        uint32_t hiddenBit = (uint32_t)1 << (uint32_t)significandBitCount;
        uint32_t fullSignificand = significandBitPattern + hiddenBit;
        significand = fullSignificand << (32 - significandBitCount - 1);
    }

    static const uint32_t halfUlp = (uint32_t)1 << (32 - significandBitCount - 2);
    uint32_t upperMidpointExact = significand + halfUlp;

    int isBoundary = significandBitPattern == 0;
    static const uint32_t quarterUlp = halfUlp >> 1;
    uint32_t lowerMidpointExact = significand - (isBoundary ? quarterUlp : halfUlp);
    int base10Exponent = decimalExponentFor2ToThe(binaryExponent);

    uint64_t powerOfTenRoundedDown = 0;
    uint64_t powerOfTenRoundedUp = 0;
    int powerOfTenExponent = 0;
    intervalContainingPowerOf10_Float(-base10Exponent,
                                      &powerOfTenRoundedDown,
                                      &powerOfTenRoundedUp,
                                      &powerOfTenExponent);
    const int extraBits = binaryExponent + powerOfTenExponent;

    static const int integerBits = 5;
    const int shift = integerBits - extraBits;
    const int roundUpBias = (1 << shift) - 1;
    static const int fractionBits = 64 - integerBits;
    uint64_t u, l;
    if (significandBitPattern & 1) {
        uint64_t u1 = multiply64x32RoundingDown(powerOfTenRoundedDown, upperMidpointExact);
        u = u1 >> shift;
        uint64_t l1 = multiply64x32RoundingUp(powerOfTenRoundedUp, lowerMidpointExact);
        l = (l1 + roundUpBias) >> shift;
    } else {
        uint64_t u1 = multiply64x32RoundingUp(powerOfTenRoundedUp, upperMidpointExact);
        u = (u1 + roundUpBias) >> shift;
        uint64_t l1 = multiply64x32RoundingDown(powerOfTenRoundedDown, lowerMidpointExact);
        l = l1 >> shift;
    }

    static const uint64_t fixedPointOne = (uint64_t)1 << fractionBits;
    static const uint64_t fixedPointMask = fixedPointOne - 1;
    uint64_t t = u;
    uint64_t delta = u - l;
    int exponent = base10Exponent + 1;
    while (t < fixedPointOne) {
        exponent -= 1;
        delta *= 10;
        t *= 10;
    }
    int8_t *digit_p = digits;
    int nextDigit = (int)(t >> fractionBits);
    t &= fixedPointMask;
    while (t > delta) {
        *digit_p++ = nextDigit;
        delta *= 10;
        t *= 10;
        nextDigit = (int)(t >> fractionBits);
        t &= fixedPointMask;
    }

    if (delta > t + fixedPointOne) {
        uint64_t skew;
        if (isBoundary) {
            skew = delta - delta / 3 - t;
        } else {
            skew = delta / 2 - t;
        }
        uint64_t one = (uint64_t)(1) << (64 - integerBits);
        uint64_t lastAccurateBit = 1ULL << 24;
        uint64_t fractionMask = (one - 1) & ~(lastAccurateBit - 1);
        uint64_t oneHalf = one >> 1;
        if (((skew + (lastAccurateBit >> 1)) & fractionMask) == oneHalf) {
            int adjust = (int)(skew >> (64 - integerBits));
            nextDigit = (nextDigit - adjust) & ~1;
        } else {
            int adjust = (int)((skew + oneHalf) >> (64 - integerBits));
            nextDigit = (nextDigit - adjust);
        }
    }
    *digit_p++ = nextDigit;

    *decimalExponent = exponent;
    return digit_p - digits;
}

int swift_decompose_float80(long double d, int8_t *digits, size_t digits_length, int *decimalExponent) {
    static const int exponentBitCount = 15;
    static const int exponentMask = (1 << exponentBitCount) - 1;
    static const int64_t exponentBias = (1 << (exponentBitCount - 1)) - 2;

    const uint64_t *raw_p = (const uint64_t *)&d;
    int exponentBitPattern = raw_p[1] & exponentMask;
    uint64_t significandBitPattern = raw_p[0];

    int64_t binaryExponent;
    uint64_t significand;
    if (digits_length < 21) {
        return 0;
    } else if (exponentBitPattern == exponentMask) {
        return 0;
    } else if (exponentBitPattern == 0) {
        if (significandBitPattern == 0) {
            digits[0] = 0;
            *decimalExponent = 0;
            return 1;
        } else {
            binaryExponent = 1 - exponentBias;
            significand = significandBitPattern;
        }
    } else if (significandBitPattern >> 63) {
        binaryExponent = exponentBitPattern - exponentBias;
        significand = significandBitPattern;
    } else {
        return 0;
    }

    uint64_t halfUlp = (uint64_t)1 << 63;
    uint64_t quarterUlp = halfUlp >> 1;
    uint64_t threeQuarterUlp = halfUlp + quarterUlp;
    swift_uint128_t upperMidpointExact, lowerMidpointExact;
    initialize128WithHighLow64(upperMidpointExact, significand, halfUlp);
    int isBoundary = (significandBitPattern & 0x7fffffffffffffff) == 0;
    initialize128WithHighLow64(lowerMidpointExact, significand - 1, isBoundary ? threeQuarterUlp : halfUlp);
    int base10Exponent = decimalExponentFor2ToThe(binaryExponent);
    swift_uint192_t powerOfTenRoundedDown;
    swift_uint192_t powerOfTenRoundedUp;
    int powerOfTenExponent = 0;
    intervalContainingPowerOf10_Float80(-base10Exponent,
                                        &powerOfTenRoundedDown,
                                        &powerOfTenRoundedUp,
                                        &powerOfTenExponent);
    const int extraBits = binaryExponent + powerOfTenExponent;
    static const int integerBits = 14;
    static const int fractionBits = 192 - integerBits;
    static const int highFractionBits = fractionBits % 64;
    swift_uint192_t u, l;
    if (significandBitPattern & 1) {
        u = powerOfTenRoundedDown;
        multiply192x128RoundingDown(&u, upperMidpointExact);
        shiftRightRoundingDown192(&u, integerBits - extraBits);

        l = powerOfTenRoundedUp;
        multiply192x128RoundingUp(&l, lowerMidpointExact);
        shiftRightRoundingUp192(&l, integerBits - extraBits);
    } else {
        u = powerOfTenRoundedUp;
        multiply192x128RoundingUp(&u, upperMidpointExact);
        shiftRightRoundingUp192(&u, integerBits - extraBits);

        l = powerOfTenRoundedDown;
        multiply192x128RoundingDown(&l, lowerMidpointExact);
        shiftRightRoundingDown192(&l, integerBits - extraBits);
    }

    static const uint64_t fixedPointOneHigh = (uint64_t)1 << highFractionBits;
    static const uint64_t fixedPointMaskHigh = fixedPointOneHigh - 1;
    swift_uint192_t t = u;
    swift_uint192_t delta = u;
    subtract192x192(&delta, l);
    int exponent = base10Exponent + 1;
    while (t.high < fixedPointOneHigh) {
        exponent -= 1;
        multiply192xi32(&delta, 10);
        multiply192xi32(&t, 10);
    }
    int8_t *digit_p = digits;
    int nextDigit = (int)(t.high >> highFractionBits);
    t.high &= fixedPointMaskHigh;
    swift_uint192_t d0 = delta;
    swift_uint192_t t0 = t;
    multiply192xi32(&d0, 10000);
    multiply192xi32(&t0, 10000);
    int fourDigits = (int)(t0.high >> highFractionBits);
    t0.high &= fixedPointMaskHigh;
    while (isLessThan192x192(d0, t0)) {
        *digit_p++ = nextDigit;
        int d2 = fourDigits / 100;
        *digit_p++ = d2 / 10;
        *digit_p++ = d2 % 10;
        d2 = fourDigits % 100;
        *digit_p++ = d2 / 10;
        nextDigit = d2 % 10;
        t = t0;
        delta = d0;
        multiply192xi32(&d0, 10000);
        multiply192xi32(&t0, 10000);
        fourDigits = (int)(t0.high >> highFractionBits);
        t0.high &= fixedPointMaskHigh;
    }

    while (isLessThan192x192(delta, t)) {
        *digit_p++ = nextDigit;
        multiply192xi32(&delta, 10);
        multiply192xi32(&t, 10);
        nextDigit = (int)(t.high >> highFractionBits);
        t.high &= fixedPointMaskHigh;
    }

    uint64_t deltaHigh64 = delta.high;
    uint64_t tHigh64 = t.high;
    if (deltaHigh64 > tHigh64 + ((uint64_t)1 << (fractionBits % 64))) {
        uint64_t skew;
        if (isBoundary) {
            skew = deltaHigh64 - deltaHigh64 / 3 - tHigh64;
        } else {
            skew = deltaHigh64 / 2 - tHigh64;
        }
        uint64_t one = (uint64_t)(1) << (64 - integerBits);
        uint64_t fractionMask = one - 1;
        uint64_t oneHalf = one >> 1;
        if ((skew & fractionMask) == oneHalf) {
            int adjust = (int)(skew >> (64 - integerBits));
            nextDigit = (nextDigit - adjust) & ~1;
        } else {
            int adjust = (int)((skew + oneHalf) >> (64 - integerBits));
            nextDigit = (nextDigit - adjust);
        }
    }
    *digit_p++ = nextDigit;

    *decimalExponent = exponent;
    return digit_p - digits;
}

static size_t swift_format_constant(char *dest, size_t length, const char *s) {
    const size_t l = strlen(s);
    if (length <= l) {
        return 0;
    }
    strcpy(dest, s);
    return l;
}

size_t swift_format_float(float d, char *dest, size_t length)
{
    if (!isfinite(d)) {
        if (isinf(d)) {
            if (signbit(d)) {
                return swift_format_constant(dest, length, "-inf");
            } else {
                return swift_format_constant(dest, length, "inf");
            }
        } else {
            static const int significandBitCount = 23;
            uint32_t raw = bitPatternForFloat(d);
            const char *sign = signbit(d) ? "-" : "";
            const char *signaling = ((raw >> (significandBitCount - 1)) & 1) ? "" : "s";
            uint32_t payload = raw & ((1L << (significandBitCount - 2)) - 1);
            char buff[32];
            if (payload != 0) {
                snprintf(buff, sizeof(buff), "%s%snan(0x%x)",
                         sign, signaling, payload);
            } else {
                snprintf(buff, sizeof(buff), "%s%snan",
                         sign, signaling);
            }
            return swift_format_constant(dest, length, buff);
        }
    }

    if (d == 0.0) {
        if (signbit(d)) {
            return swift_format_constant(dest, length, "-0.0");
        } else {
            return swift_format_constant(dest, length, "0.0");
        }
    }

    int decimalExponent;
    int8_t digits[9];
    int digitCount =
        swift_decompose_float(d, digits, sizeof(digits), &decimalExponent);
    if (decimalExponent < -3 || fabsf(d) > (1<<24)) {
        return swift_format_exponential(dest, length, signbit(d),
                 digits, digitCount, decimalExponent);
    } else {
        return swift_format_decimal(dest, length, signbit(d),
                 digits, digitCount, decimalExponent);
    }
}

size_t swift_format_double(double d, char *dest, size_t length)
{
    if (!isfinite(d)) {
        if (isinf(d)) {
            if (signbit(d)) {
                return swift_format_constant(dest, length, "-inf");
            } else {
                return swift_format_constant(dest, length, "inf");
            }
        } else {
            static const int significandBitCount = 52;
            uint64_t raw = bitPatternForDouble(d);
            const char *sign = signbit(d) ? "-" : "";
            const char *signaling = ((raw >> (significandBitCount - 1)) & 1) ? "" : "s";
            uint64_t payload = raw & ((1ull << (significandBitCount - 2)) - 1);
            char buff[32];
            if (payload != 0) {
                snprintf(buff, sizeof(buff), "%s%snan(0x%" PRIx64 ")",
                         sign, signaling, payload);
            } else {
                snprintf(buff, sizeof(buff), "%s%snan",
                         sign, signaling);
            }
            return swift_format_constant(dest, length, buff);
        }
    }

    if (d == 0.0) {
        if (signbit(d)) {
            return swift_format_constant(dest, length, "-0.0");
        } else {
            return swift_format_constant(dest, length, "0.0");
        }
    }

    int decimalExponent;
    int8_t digits[17];
    int digitCount = swift_decompose_double(d, digits, sizeof(digits), &decimalExponent);
    if (decimalExponent < -3 || fabs(d) > (1LL<<53)) {
        return swift_format_exponential(dest, length, signbit(d),
                 digits, digitCount, decimalExponent);
    } else {
        return swift_format_decimal(dest, length, signbit(d),
                 digits, digitCount, decimalExponent);
    }
}

size_t swift_format_float80(long double d, char *dest, size_t length)
{
    if (!isfinite(d)) {
        if (isinf(d)) {
            if (signbit(d)) {
                return swift_format_constant(dest, length, "-inf");
            } else {
                return swift_format_constant(dest, length, "inf");
            }
        } else {
            uint64_t significandBitPattern = *(const uint64_t *)&d;
            const char *sign = signbit(d) ? "-" : "";
            const char *signaling = ((significandBitPattern >> 62) & 1) ? "" : "s";
            uint64_t payload = significandBitPattern & (((uint64_t)1 << 61) - 1);
            char buff[32];
            if (payload != 0) {
                snprintf(buff, sizeof(buff), "%s%snan(0x%" PRIx64 ")",
                         sign, signaling, payload);
            } else {
                snprintf(buff, sizeof(buff), "%s%snan",
                         sign, signaling);
            }
            return swift_format_constant(dest, length, buff);
        }
    }

    if (d == 0.0) {
        if (signbit(d)) {
            return swift_format_constant(dest, length, "-0.0");
        } else {
            return swift_format_constant(dest, length, "0.0");
        }
    }

    int decimalExponent;
    int8_t digits[21];
    int digitCount =
        swift_decompose_float80(d, digits, sizeof(digits), &decimalExponent);
    if (decimalExponent < -3 || fabsl(d) > 0x1.0p64L) {
        return swift_format_exponential(dest, length, signbit(d),
                 digits, digitCount, decimalExponent);
    } else {
        return swift_format_decimal(dest, length, signbit(d),
                 digits, digitCount, decimalExponent);
    }
}

size_t swift_format_exponential(char *dest, size_t length,
    bool negative, const int8_t *digits, int digit_count, int exponent)
{
    size_t maximum_size = digit_count + 9;
    if (length < maximum_size) {
        size_t actual_size =
            + (negative ? 1 : 0)
            + digit_count
            + (digit_count > 1 ? 1 : 0)
            + 1
            + 1
            + (exponent > 99 ? (exponent > 999 ? 4 : 3) : 2)
            + 1;
        if (length < actual_size) {
            if (length > 0) {
                dest[0] = 0;
            }
            return 0;
        }
    }
    char *p = dest;
    if (negative) {
        *p++ = '-';
    }

    *p++ = digits[0] + '0';
    exponent -= 1;
    if (digit_count > 1) {
        *p++ = '.';
        for (int i = 1; i < digit_count; i++) {
            *p++ = digits[i] + '0';
        }
    }
    *p++ = 'e';
    if (exponent < 0) {
        *p++ = '-';
        exponent = -exponent;
    } else {
        *p++ = '+';
    }
    if (exponent > 99) {
        if (exponent > 999) {
            *p++ = (exponent / 1000 % 10) + '0';
        }
        *p++ = (exponent / 100 % 10) + '0';
        exponent %= 100;
    }
    *p++ = (exponent / 10) + '0';
    *p++ = (exponent % 10) + '0';
    *p = '\0';
    return p - dest;
}

size_t swift_format_decimal(char *dest, size_t length,
    bool negative, const int8_t *digits, int digit_count, int exponent)
{
    size_t maximum_size =
        digit_count
        + (exponent > 0 ? exponent : -exponent)
        + 4;
    if (length < maximum_size) {
        if (exponent <= 0) {
            size_t actual_size =
                (negative ? 1 : 0)
                + 2
                + (-exponent)
                + digit_count
                + 1;
            if (length < actual_size) {
                if (length > 0) {
                    dest[0] = 0;
                }
                return 0;
            }
        } else if (exponent < digit_count) {
            size_t actual_size =
                (negative ? 1 : 0)
                + digit_count
                + 1
                + 1;
            if (length < actual_size) {
                if (length > 0) {
                    dest[0] = 0;
                }
                return 0;
            }
        } else {
            size_t actual_size =
                (negative ? 1 : 0)
                + digit_count
                + (exponent - digit_count)
                + 2
                + 1;
            if (length < actual_size) {
                if (length > 0) {
                    dest[0] = 0;
                }
                return 0;
            }
        }
    }

    char *p = dest;
    if (negative) {
        *p++ = '-';
    }

    if (exponent <= 0) {
        *p++ = '0';
        *p++ = '.';
        while (exponent < 0) {
            *p++ = '0';
            exponent += 1;
        }
        for (int i = 0; i < digit_count; ++i) {
            *p++ = digits[i] + '0';
        }
    } else if (exponent < digit_count) {
        for (int i = 0; i < digit_count; i++) {
            if (exponent == 0) {
                *p++ = '.';
            }
            *p++ = digits[i] + '0';
            exponent -= 1;
        }
    } else {
        for (int i = 0; i < digit_count; i++) {
            *p++ = digits[i] + '0';
            exponent -= 1;
        }
        while (exponent > 0) {
            *p++ = '0';
            exponent -= 1;
        }
        *p++ = '.';
        *p++ = '0';
    }
    *p = '\0';
    return p - dest;
}

static uint64_t multiply64x32RoundingDown(uint64_t lhs, uint32_t rhs) {
    static const uint64_t mask32 = UINT32_MAX;
    uint64_t t = ((lhs & mask32) * rhs) >> 32;
    return t + (lhs >> 32) * rhs;
}

static uint64_t multiply64x32RoundingUp(uint64_t lhs, uint32_t rhs) {
    static const uint64_t mask32 = UINT32_MAX;
    uint64_t t = (((lhs & mask32) * rhs) + mask32) >> 32;
    return t + (lhs >> 32) * rhs;
}

static uint64_t multiply64x64RoundingDown(uint64_t lhs, uint64_t rhs) {
    __uint128_t full = (__uint128_t)lhs * rhs;
    return (uint64_t)(full >> 64);
}

static uint64_t multiply64x64RoundingUp(uint64_t lhs, uint64_t rhs) {
    static const __uint128_t roundingUpBias = ((__uint128_t)1 << 64) - 1;
    __uint128_t full = (__uint128_t)lhs * rhs;
    return (uint64_t)((full + roundingUpBias) >> 64);
}

static swift_uint128_t multiply128x64RoundingDown(swift_uint128_t lhs, uint64_t rhs) {
    uint64_t lhsl = (uint64_t)lhs;
    uint64_t lhsh = (uint64_t)(lhs >> 64);
    swift_uint128_t h = (swift_uint128_t)lhsh * rhs;
    swift_uint128_t l = (swift_uint128_t)lhsl * rhs;
    return h + (l >> 64);
}

static swift_uint128_t multiply128x64RoundingUp(swift_uint128_t lhs, uint64_t rhs) {
    uint64_t lhsl = (uint64_t)lhs;
    uint64_t lhsh = (uint64_t)(lhs >> 64);
    swift_uint128_t h = (swift_uint128_t)lhsh * rhs;
    swift_uint128_t l = (swift_uint128_t)lhsl * rhs;
    const static __uint128_t bias = ((__uint128_t)1 << 64) - 1;
    return h + ((l + bias) >> 64);
}

static swift_uint128_t shiftRightRoundingDown128(swift_uint128_t lhs, int shift) {
    return lhs >> shift;
}

static swift_uint128_t shiftRightRoundingUp128(swift_uint128_t lhs, int shift) {
    uint64_t bias = ((uint64_t)1 << shift) - 1;
    return ((lhs + bias) >> shift);
}

static void multiply192x64RoundingDown(swift_uint192_t *lhs, uint64_t rhs) {
    __uint128_t cd = (__uint128_t)lhs->low * rhs;
    __uint128_t bc = (__uint128_t)lhs->mid * rhs;
    __uint128_t ab = (__uint128_t)lhs->high * rhs;
    __uint128_t c = (cd >> 64) + (uint64_t)bc;
    __uint128_t b = (bc >> 64) + (uint64_t)ab + (c >> 64);
    __uint128_t a = (ab >> 64) + (b >> 64);
    lhs->high = a;
    lhs->mid = b;
    lhs->low = c;
}

static void multiply192x64RoundingUp(swift_uint192_t *lhs, uint64_t rhs) {
    __uint128_t cd = (__uint128_t)lhs->low * rhs + UINT64_MAX;
    __uint128_t bc = (__uint128_t)lhs->mid * rhs;
    __uint128_t ab = (__uint128_t)lhs->high * rhs;
    __uint128_t c = (cd >> 64) + (uint64_t)bc;
    __uint128_t b = (bc >> 64) + (uint64_t)ab + (c >> 64);
    __uint128_t a = (ab >> 64) + (b >> 64);
    lhs->high = a;
    lhs->mid = b;
    lhs->low = c;
}

static void multiply192xi32(swift_uint192_t *lhs, uint32_t rhs) {
    __uint128_t t = (__uint128_t)lhs->low * rhs;
    lhs->low = (uint64_t)t;
    t = (t >> 64) + (__uint128_t)lhs->mid * rhs;
    lhs->mid = (uint64_t)t;
    t = (t >> 64) + (__uint128_t)lhs->high * rhs;
    lhs->high = (uint64_t)t;
}

static void multiply192x128RoundingDown(swift_uint192_t *lhs, swift_uint128_t rhs) {
    __uint128_t current = (__uint128_t)lhs->low * (uint64_t)rhs;

    current = (current >> 64);
    __uint128_t t = (__uint128_t)lhs->low * (rhs >> 64);
    current += (uint64_t)t;
    __uint128_t next = t >> 64;
    t = (__uint128_t)lhs->mid * (uint64_t)rhs;
    current += (uint64_t)t;
    next += t >> 64;

    current = next + (current >> 64);
    t = (__uint128_t)lhs->mid * (rhs >> 64);
    current += (uint64_t)t;
    next = t >> 64;
    t = (__uint128_t)lhs->high * (uint64_t)rhs;
    current += (uint64_t)t;
    next += t >> 64;
    lhs->low = (uint64_t)current;

    current = next + (current >> 64);
    t = (__uint128_t)lhs->high * (rhs >> 64);
    current += t;
    lhs->mid = (uint64_t)current;
    lhs->high = (uint64_t)(current >> 64);
}

static void multiply192x128RoundingUp(swift_uint192_t *lhs, swift_uint128_t rhs) {
    swift_uint128_t current = (swift_uint128_t)lhs->low * (uint64_t)rhs;
    current += UINT64_MAX;

    current = (current >> 64);
    swift_uint128_t t = (swift_uint128_t)lhs->low * (rhs >> 64);
    current += (uint64_t)t;
    swift_uint128_t next = t >> 64;
    t = (swift_uint128_t)lhs->mid * (uint64_t)rhs;
    current += (uint64_t)t;
    next += t >> 64;
    current += UINT64_MAX;

    current = next + (current >> 64);
    t = (swift_uint128_t)lhs->mid * (rhs >> 64);
    current += (uint64_t)t;
    next = t >> 64;
    t = (swift_uint128_t)lhs->high * (uint64_t)rhs;
    current += (uint64_t)t;
    next += t >> 64;
    lhs->low = (uint64_t)current;

    current = next + (current >> 64);
    t = (swift_uint128_t)lhs->high * (rhs >> 64);
    current += t;
    lhs->mid = (uint64_t)current;
    lhs->high = (uint64_t)(current >> 64);
}

static void subtract192x192(swift_uint192_t *lhs, swift_uint192_t rhs) {
    swift_uint128_t t = (swift_uint128_t)lhs->low + (~rhs.low) + 1;
    lhs->low = t;
    t = (t >> 64) + lhs->mid + (~rhs.mid);
    lhs->mid = t;
    lhs->high += (t >> 64) + (~rhs.high);
}

static int isLessThan192x192(swift_uint192_t lhs, swift_uint192_t rhs) {
    return (lhs.high < rhs.high)
        || (lhs.high == rhs.high
            && (lhs.mid < rhs.mid
                || (lhs.mid == rhs.mid
                    && lhs.low < rhs.low)));
}

static void shiftRightRoundingDown192(swift_uint192_t *lhs, int shift) {
    __uint128_t t = (__uint128_t)lhs->low >> shift;
    t += ((__uint128_t)lhs->mid << (64 - shift));
    lhs->low = t;
    t >>= 64;
    t += ((__uint128_t)lhs->high << (64 - shift));
    lhs->mid = t;
    t >>= 64;
    lhs->high = t;
}

static void shiftRightRoundingUp192(swift_uint192_t *lhs, int shift) {
    const uint64_t bias = (1 << shift) - 1;
    __uint128_t t = ((__uint128_t)lhs->low + bias) >> shift;
    t += ((__uint128_t)lhs->mid << (64 - shift));
    lhs->low = t;
    t >>= 64;
    t += ((__uint128_t)lhs->high << (64 - shift));
    lhs->mid = t;
    t >>= 64;
    lhs->high = t;
}


static const uint64_t powersOf10_Float[40] = {
    0x8000000000000000, 0xa000000000000000, 0xc800000000000000, 0xfa00000000000000, 0x9c40000000000000, 0xc350000000000000,
    0xf424000000000000, 0x9896800000000000, 0xbebc200000000000, 0xee6b280000000000, 0x9502f90000000000, 0xba43b74000000000,
    0xe8d4a51000000000, 0x9184e72a00000000, 0xb5e620f480000000, 0xe35fa931a0000000, 0x8e1bc9bf04000000, 0xb1a2bc2ec5000000,
    0xde0b6b3a76400000, 0x8ac7230489e80000, 0xad78ebc5ac620000, 0xd8d726b7177a8000, 0x878678326eac9000, 0xa968163f0a57b400,
    0xd3c21bcecceda100, 0x84595161401484a0, 0xa56fa5b99019a5c8, 0xcecb8f27f4200f3a, 0x813f3978f8940984, 0xa18f07d736b90be5,
    0xc9f2c9cd04674ede, 0xfc6f7c4045812296, 0x9dc5ada82b70b59d, 0xc5371912364ce305, 0xf684df56c3e01bc6, 0x9a130b963a6c115c,
    0xc097ce7bc90715b3, 0xf0bdc21abb48db20, 0x96769950b50d88f4, 0xbc143fa4e250eb31,
};

static const uint64_t powersOf10_Double[] = {
    0x3931b850df08e738, 0x95fe7e07c91efafa, 0xba954f8e758fecb3, 0x9774919ef68662a3, 0x9028bed2939a635c, 0x98ee4a22ecf3188b,
    0x47b233c92125366e, 0x9a6bb0aa55653b2d, 0x4ee367f9430aec32, 0x9becce62836ac577, 0x6f773fc3603db4a9, 0x9d71ac8fada6c9b5,
    0xc47bc5014a1a6daf, 0x9efa548d26e5a6e1, 0x80e8a40eccd228a4, 0xa086cfcd97bf97f3, 0xb8ada00e5a506a7c, 0xa21727db38cb002f,
    0xc13e60d0d2e0ebba, 0xa3ab66580d5fdaf5, 0xc2974eb4ee658828, 0xa54394fe1eedb8fe, 0xcb4ccd500f6bb952, 0xa6dfbd9fb8e5b88e,
    0x3f2398d747b36224, 0xa87fea27a539e9a5, 0xdde50bd1d5d0b9e9, 0xaa242499697392d2, 0xfdc20d2b36ba7c3d, 0xabcc77118461cefc,
    0x0000000000000000, 0xad78ebc5ac620000, 0x9670b12b7f410000, 0xaf298d050e4395d6, 0x3b25a55f43294bcb, 0xb0de65388cc8ada8,
    0x58edec91ec2cb657, 0xb2977ee300c50fe7, 0x29babe4598c311fb, 0xb454e4a179dd1877, 0x577b986b314d6009, 0xb616a12b7fe617aa,
    0x0c11ed6d538aeb2f, 0xb7dcbf5354e9bece, 0x6d953e2bd7173692, 0xb9a74a0637ce2ee1, 0x9d6d1ad41abe37f1, 0xbb764c4ca7a4440f,
    0x4b2d8644d8a74e18, 0xbd49d14aa79dbc82, 0xe0470a63e6bd56c3, 0xbf21e44003acdd2c, 0x505f522e53053ff2, 0xc0fe908895cf3b44,
    0xcca845ab2beafa9a, 0xc2dfe19c8c055535, 0x1027fff56784f444, 0xc4c5e310aef8aa17,
};

static const uint64_t powersOf10_Float80[] = {
    0x56825ada98468526, 0x0abc23fcfda07e29, 0x871db786569ca2dd, 0xe3885beff5ee930d, 0xd1e638f68c97f0e5, 0xde90d1b113564507,
    0x5a7d22b5236bcad4, 0xbab3a28a6f0a489c, 0xb74ea21ab2946479, 0x7d1f78bf0f4e2878, 0xcf4aea39615ffe6e, 0x96f9351fcfdd2686,
    0xad725c0d6c214314, 0x5bd5c19f18bd2857, 0xf8afafd6ef185238, 0xe418e9217ce83755, 0x801e38463183fc88, 0xccd1ffc6bba63e21,
    0x4dcd52747e029d0c, 0x867b3096b1619df8, 0xa8b11ff4721d92fb, 0xed2903f7e1df2b78, 0xc846664fe1364ee8, 0x8aefaaae9060380f,
    0xed7a7f4e1e171498, 0x7da1a627b88527f1, 0xe4dbb751faa311b0, 0x320796dc9b1a158c, 0x2a11a871597b8012, 0xbc7d620092481a7e,
    0x796014ec6f4c0dcb, 0xcfa99f62903708d7, 0x9b3dee433c1311e9, 0x08920ae76bdb8282, 0x952b06c385a08ff6, 0xffb7a402531fd4c9,
    0x18faa162f2c4d6b9, 0x050be8c5d21c6db6, 0xd29c7528965ae5bd, 0x576a6c1abab7f7e7, 0xc05fb0b5c550f28d, 0xad7617610634129e,
    0x6cc3b2fb9cae4875, 0xe1bd5b09b7202157, 0x8edd4417ae3dc210, 0xfafc1dc8fd12a6f8, 0xc3f29d230036529b, 0xeb542860da9bc7d8,
    0x9d198c56bc799f35, 0x42960adf9591f02a, 0xc1d1a4b6bc2eafc8, 0x1803d096e43ff4bc, 0xdef9759d6432dab7, 0x9fa18c5de65a16fb,
    0x74872779576a577c, 0x9150140eb5101a96, 0x83793dfc83fd2f9b, 0x415d0667f0f88262, 0x4898d98d1314d99f, 0xd890d45a257f4644,
    0x30a5256a610c1c72, 0x6ebca4d0365d504d, 0xb25d93f98145bdab, 0xfb149b1f86a46376, 0xb5143323a7f8e16c, 0x92e74be10524c389,
    0x7b7532e25fead4c8, 0x0df6ab8ac6a0ec2f, 0xf1fb6e82e4df4a77, 0x3b738d6a3caae67a, 0x346b2dd31826cfed, 0xc74c79bce7fe949f,
    0x22d3a12777cee527, 0xe185ac46f6ef1993, 0xa424ef0bb5ad3129, 0xb7b6bccb9f60adec, 0x30ad7df78fe30cc8, 0x8730d40821cd89f3,
    0x21049149d72d44d5, 0x1e86debc54dd290d, 0xdeb04cb82aec22cb, 0xeb69d287f5e7f920, 0x8d7e84a13801d034, 0xb7688f9800d26b3a,
    0x47b3da9aeff7df71, 0x82678774d6c0ac59, 0x970e8fd25ead01e3, 0x50d3e92e2e4f8210, 0x9492db3d978aaca8, 0xf8d2dcaf37504b51,
    0xf4ce72058f777a4c, 0xb9eb2c585e924efe, 0xcceef83fdedcc506, 0xb0e624a35b791884, 0x7104a98dbbb38b94, 0xa8c8fc3b03c3d1ed,
    0x6c94ecd8291e2ac9, 0x978b03821014b68c, 0x8b03518396007c08, 0x96475208daa03ee3, 0x10eaa1481b149e5a, 0xe4fc163319551441,
    0xd709a820ff4ac847, 0x75aab7cb5cb15414, 0xbc980b270680156a, 0xf273bca6b4de9e24, 0xb5025d88d4b252e1, 0x9b53e384eccabcf7,
    0x6c55a0603a928f40, 0x9e20db16dfb6b461, 0xffdbcf7251089796, 0x9006db4d43cffe42, 0x9b4bca4cd6cec2db, 0xd2ba3f510a3aa638,
    0xa6b3c457fd0cd4d6, 0x28a4de91ba868fbf, 0xad8ea05a5f27642a, 0xe7b14ed140f8d98e, 0x7b2f7d61ce5d426c, 0x8ef179291b6f5424,
    0x4a964d052fd03e10, 0x06897060bf491e6e, 0xeb75718d285cd8bf, 0x22b2270f0e8dd87c, 0xa8510fa2f5a9e4de, 0xc1ed0ed498f7c54c,
    0x09102915726a9905, 0x5a0eb896edc89b54, 0x9fb8208d65ea5eda, 0xb80d5f481d01deb9, 0x673f2aa50486f5ba, 0x838bd699b7c539e6,
    0x668b62b20ec2633b, 0x8682604c7123f859, 0xd8af761f94d2db2c, 0xb2adaed8559cc199, 0x712339ba54f12372, 0xb276ce87987995d5,
    0x6beb873308685711, 0xac1ce34246ed56ad, 0x92fc133455668c02, 0x593293d68a2261bc, 0x3c368f9497ca075d, 0xf21da89a29fa1c61,
    0x854051f9f0e4ca66, 0x8c5d5a234eda57f7, 0xc768aa46d6d1b675, 0x333d09b2299c5e6b, 0xcf1f49c33399c5ac, 0xa43c26a751d4f7e7,
    0x25a440d8b1620532, 0x274ebc67c3e21943, 0x8743f33df0feed29, 0x3ca95e3deb5be648, 0x52d18ccca1c558c2, 0xdecfcc3329238dd8,
    0x59d1a7704af3acd7, 0xfae7722c6af19467, 0xb78280c024488353, 0x78813f3e80148049, 0x73b2baf13aa1c233, 0x9723ed8a28baf5ac,
    0xf296a8198aa40fb8, 0x235532b08487fe6a, 0xf8f60e812de0cd7d, 0xa7fbdcb40b4f648f, 0x4f20ba9a64a7f6e7, 0xcd0bf4d206072167,
    0x8dbf63ea468c724f, 0xa0e25c08b5c189d6, 0xa8e0dbe18ffb82cf, 0x765995c6cfd406ce, 0x4c3bcb5021afcc31, 0x8b16fb203055ac76,
    0xe1d09ab6fb409872, 0x82b7e12780e7401a, 0xe51c79a85916f484, 0x89cbe2422f9e1df9, 0x7415d448f6b6f0e7, 0xbcb2b812db11a5de,
    0x605fe83842e4d290, 0xc986afbe3ee11aba, 0x9b69dbe1b548ce7c, 0x0000000000000000, 0x0000000000000000, 0x8000000000000000,
    0x0d6953169e1c7a1e, 0xf50a3fa490c30190, 0xd2d80db02aabd62b, 0x3720b80c7d8ee39d, 0xaf561aa79a10ae6a, 0xada72ccc20054ae9,
    0x7cb3f026a212df74, 0x29cb4d87f2a7400e, 0x8f05b1163ba6832d, 0x7dda22f9451d28a4, 0xe41c5bd18c57e88f, 0xeb96bf6ebadf77d8,
    0xd5da00e6e2d05e5d, 0x5e510c5a752f0f8e, 0xc2087cd3215a16ad, 0x5603ba353e0b2fac, 0x48bbddc4d7359e49, 0x9fceb7ee780436f0,
    0x15ceea8df15e47c7, 0x6a83c85cf158c652, 0x839e71d847c1779e, 0x514478d1fcd48eea, 0x3a4181cdda0d6e24, 0xd8ce1c3a2fffaea7,
    0xe8b634620f1062be, 0x7304c7fb8a2f8a8a, 0xb2900ca735bdf121, 0xc3ec2fd9302c9bda, 0x729a6a7e830e1cf2, 0x9310dd78089bd66f,
    0x1750ef5f751be079, 0x52ccabc96fc88a23, 0xf23fe788c763dffa, 0xaa80925ec1c80b65, 0x97681c548ff6c12f, 0xc784decd820a6180,
    0xb4212d4b435a2317, 0x8df0a55abbb2c99a, 0xa453618b9dfd92db, 0x939c2fedd434642a, 0x7d7de34bf5aa96b4, 0x875715282612729b,
    0xb7d9a0e46bbebb36, 0x3b2057dea52d686b, 0xdeef5022af37f1f6, 0x931a74148ea64e59, 0xfe7fe67bd1074d0c, 0xb79c7593a1c17df0,
    0xabd20df0f1f1ad54, 0x0fff83cc7fa6b77b, 0x97394e479b6573b1, 0x25f2467421674b7a, 0x828ff55a248bc026, 0xf919454d86f16685,
    0xe32dbd7131e6ab7d, 0xf674dd4821982084, 0xcd28f57dc585d094, 0x866ab816a532b07d, 0xdc567471f9639b4e, 0xa8f8bee890f905c7,
    0xaca3a975993a2626, 0xc41cf207a71d87e4, 0x8b2aa784c405e2f1, 0x731f0b7d820918bd, 0x355fde18e8448607, 0xe53ce1b25fb31788,
    0x0be7c29568db3f20, 0xc1328a3f1bf4d2b8, 0xbccd68c49888be61, 0x9c6e0b1b927b7d3f, 0xffc9b96619da642a, 0x9b7fd75a060350cd,
    0x2a84c8fb4bd2edc9, 0xa679df45d339389b, 0x80121ad60ca2c518, 0x0d4648d0876cf1c3, 0x48c67661c087fb5a, 0xd2f5e0469040e0eb,
    0xfe2d99a281a011ac, 0x65c13361e6b2c078, 0xadbfbcb6c676a69b, 0xf4ec157aa4147562, 0xac89bfa5e79484a6, 0x8f19ebdf7661e3e9,
    0xe945f8c80090be1f, 0x9b8672e64aadbed2, 0xebb812063c9e01db, 0xca12d8ad3b36d2e4, 0xd252322ea50ad274, 0xc223eeb2e1bde452,
    0xf554fb41e5b3e384, 0x977acb4d4af624fc, 0x9fe55281904ba38b, 0xf3f69093398e2573, 0x111ae5735ec0e878, 0x83b10fb893300cde,
    0x30f65a8da0d10429, 0x1eecf4cf8a0b25f5, 0xd8ecc6aa93e876fc, 0xa577f5f0c9f1e5a7, 0x91a5430ed623abf0, 0xb2a94e58da4930c3,
    0x202d2f87585ec0d7, 0x7a63589863efd480, 0x9325aaac89304b57, 0x049544fbba3c01a1, 0x53accb0f60ac6095, 0xf2622b4f6c68d6ce,
    0x268a0d5f9d5ce861, 0xfe40703e1a91de57, 0xc7a117517a09153b, 0xb6cd470a2a3b1d63, 0x5f963916b20ea587, 0xa46a9fb9111003bc,
    0xa3101c09fd8e6e96, 0xa2c6328011db5211, 0x876a39c722f798a7, 0x8507e5fdb0ec5d83, 0x7ce93cc7f8feeed4, 0xdf0ed8875e7b8914,
    0xe19b7ebe4c7bfbca, 0x40930d1129943838, 0xb7b66e12fe1af499, 0xc90c4ec15c21a357, 0xd91c86512d147305, 0x974eb20b241a65f6,
    0xb90341199c02a4eb, 0x69e684f53db6e8ce, 0xf93c8114f6c31f8a, 0xa873f1318cef91cb, 0xbf8718466b31a7ca, 0xcd45fa43b1ce4c8e,
    0xacfe0dcc5262e273, 0x6e1bbb68662fd27a, 0xa910a550810203f8, 0x59e7c8921bbe3758, 0x834743c5eab7dcea, 0x8b3e56b1b5c57589,
    0x145eed64fda2e6af, 0x1c605bdcc764238f, 0xe55d4e51d30b5592, 0xb747164b17268ea2, 0xd8aa19f1d85da07d, 0xbce81d3cc784a1ca,
    0x3666af1cb2f0356b, 0xc8dd55687a68bb70, 0x9b95d5ee4f80366d, 0x70d67261b5bde1e9, 0x1d76f2d15166ec20, 0x8024383bab19730d,
    0x084a3ba0b748546a, 0xc67f9026f83dca47, 0xd313b714d3a1c65e, 0x4411a8127eea085e, 0x441eb397ffcdab0d, 0xadd8501ad0361d15,
    0x7b62a54ed6233032, 0x75458a1c8300e014, 0x8f2e2985332eae98, 0x162d5b51a1dd9594, 0x655bb1b7aa4e8196, 0xebd96954582af06f,
    0x55e6c62f920d3682, 0x79fd57cf7c37941c, 0xc23f6474669f4abe, 0x19482fa0ac45669c, 0x803c1cd864033781, 0x9ffbf04722750449,
    0xa412d1f95f4624cd, 0xc95abe9ce589e048, 0x83c3b03af95c9674, 0xc1207e487c57b4e1, 0xf93dd2c7669a8ed1, 0xd90b75715d861b38,
    0xeb20d9a25e0372bd, 0xb5073df6adc221b4, 0xb2c2939d0763fcac, 0x1a648c339e28cc45, 0xbd14f0fa3e24b6ae, 0x933a7ad2419ea0b5,
};

static int binaryExponentFor10ToThe(int p) {
    return (int)(((((int64_t)p) * 55732705) >> 24) + 1);
}

static int decimalExponentFor2ToThe(int e) {
    return (int)(((int64_t)e * 20201781) >> 26);
}

static void intervalContainingPowerOf10_Float(int p, uint64_t *lower, uint64_t *upper, int *exponent) {
    if (p < 0) {
        uint64_t base = powersOf10_Float[p + 40];
        int baseExponent = binaryExponentFor10ToThe(p + 40);
        uint64_t tenToTheMinus40 = 0x8b61313bbabce2c6;
        *upper = multiply64x64RoundingUp(base + 1, tenToTheMinus40 + 1);
        *lower = multiply64x64RoundingDown(base, tenToTheMinus40);
        *exponent = baseExponent - 132;
    } else {
        uint64_t base = powersOf10_Float[p];
        *upper = base + 1;
        *lower = base;
        *exponent = binaryExponentFor10ToThe(p);
    }
}

static void intervalContainingPowerOf10_Double(int p, swift_uint128_t *lower, swift_uint128_t *upper, int *exponent) {
    if (p >= 0 && p <= 54) {
        if (p <= 27) {

            swift_uint128_t exact;
            initialize128WithHigh64(exact, powersOf10_Float[p]);
            *upper = exact;
            *lower = exact;
            *exponent = binaryExponentFor10ToThe(p);
            return;
        } else {

            swift_uint128_t base;
            initialize128WithHigh64(base, powersOf10_Float[p - 27]);
            int baseExponent = binaryExponentFor10ToThe(p - 27);
            uint64_t extra = powersOf10_Float[27];
            int extraExponent = binaryExponentFor10ToThe(27);
            swift_uint128_t exact = multiply128x64RoundingDown(base, extra);
            *upper = exact;
            *lower = exact;
            *exponent = baseExponent + extraExponent;
            return;
        }
    }

    int index = p + 400;
    const uint64_t *base_p = powersOf10_Double + (index / 28) * 2;
    swift_uint128_t base;
    initialize128WithHighLow64(base, base_p[1], base_p[0]);
    int extraPower = index % 28;
    int baseExponent = binaryExponentFor10ToThe(p - extraPower);

    int e = baseExponent;
    if (extraPower > 0) {
        int64_t extra = powersOf10_Float[extraPower];
        e += binaryExponentFor10ToThe(extraPower);
        *lower = multiply128x64RoundingDown(base, extra);
        increment128(base);
        *upper = multiply128x64RoundingUp(base, extra);
    } else {
        *lower = base;
        increment128(base);
        *upper = base;
    }
    *exponent = e;
}

static void intervalContainingPowerOf10_Float80(int p, swift_uint192_t *lower, swift_uint192_t *upper, int *exponent) {
    if (p >= 0 && p <= 27) {
        uint64_t exact = powersOf10_Float[p];
        initialize192WithHighMidLow64(*upper, exact, 0, 0);
        initialize192WithHighMidLow64(*lower, exact, 0, 0);
        *exponent = binaryExponentFor10ToThe(p);
        return;
    }
    int index = p + 5063;
    const uint64_t *base_p = powersOf10_Float80 + (index / 83) * 3;
    initialize192WithHighMidLow64(*upper, base_p[2], base_p[1], base_p[0] + 1);
    initialize192WithHighMidLow64(*lower, base_p[2], base_p[1], base_p[0]);
    int extraPower = index % 83;
    int e = binaryExponentFor10ToThe(p - extraPower);
    while (extraPower > 27) {
        uint64_t power27 = powersOf10_Float[27];
        multiply192x64RoundingDown(lower, power27);
        multiply192x64RoundingUp(upper, power27);
        e += binaryExponentFor10ToThe(27);
        extraPower -= 27;
    }
    if (extraPower > 0) {
        uint64_t extra = powersOf10_Float[extraPower];
        multiply192x64RoundingDown(lower, extra);
        multiply192x64RoundingUp(upper, extra);
        e += binaryExponentFor10ToThe(extraPower);
    }
    *exponent = e;
}

}
