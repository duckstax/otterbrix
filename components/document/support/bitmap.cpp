#include "bitmap.hpp"

#ifdef _MSC_VER

#include <mutex>
#include <bitset>
#include <array>
#include <intrin.h>

namespace document {

template<typename T>
static inline int popcount_c(T v) {
    v = v - ((v >> 1) & (T)~(T)0/3);
    v = (v & (T)~(T)0/15*3) + ((v >> 2) & (T)~(T)0/15*3);
    v = (v + (v >> 4)) & (T)~(T)0/255*15;
    v = (T)(v * ((T)~(T)0/255)) >> (sizeof(T) - 1) * CHAR_BIT;
    return (int)v;
}

#if !defined(_M_ARM) && !defined(_M_ARM64)

static bool _can_popcnt = false;

static void detect_popcnt() {
    std::array<int, 4> cpui;
    __cpuid(cpui.data(), 0);
    int n_ids = cpui[0];
    if(n_ids >= 1) {
        __cpuidex(cpui.data(), 1, 0);
        std::bitset<32> ecx = cpui[2];
        _can_popcnt = ecx[23];
    }
}

static inline bool can_popcnt() {
    static std::once_flag once;
    if (!_can_popcnt)
        std::call_once(once, detect_popcnt);
    return _can_popcnt;
}

#endif

int _popcount(unsigned int v) noexcept {
#if !defined(_M_ARM) && !defined(_M_ARM64)
    return can_popcnt() ? __popcnt(v) : popcount_c(v);
#else
    return popcount_c(v);
#endif
}

int _popcountl(unsigned long v) noexcept {
#if !defined(_M_ARM) && !defined(_M_ARM64)
    return can_popcnt() ? __popcnt(v) : popcount_c(v);
#else
    return popcount_c(v);
#endif
}

int _popcountll(unsigned long long v) noexcept {
#if defined(_WIN64) && !defined(_M_ARM64)
    return can_popcnt() ? (int)__popcnt64(v) : popcount_c(v);
#else
    return popcount_c(v);
#endif
}

}

#endif
