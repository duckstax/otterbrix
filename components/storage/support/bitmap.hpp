#pragma once

#include <type_traits>

#ifndef _MSC_VER
extern "C" {
extern int __builtin_popcount(unsigned int);
extern int __builtin_popcountl(unsigned long);
extern int __builtin_popcountll(unsigned long long);
}
#endif

namespace storage {

template<class INT, typename std::enable_if<std::is_integral<INT>::value, INT>::type = 0>
INT popcount(INT bits) {
    #ifdef _MSC_VER
    extern int _popcount(unsigned int) noexcept;
    extern int _popcountl(unsigned long) noexcept;
    extern int _popcountll(unsigned long long) noexcept;
    if (sizeof(INT) <= sizeof(int))
        return _popcount(bits);
    else if (sizeof(INT) <= sizeof(long))
        return _popcountl(bits);
    else
        return _popcountll(bits);
    #else
    if (sizeof(INT) <= sizeof(int))
        return __builtin_popcount(bits);
    else if (sizeof(INT) <= sizeof(long))
        return __builtin_popcountl(bits);
    else
        return __builtin_popcountll(bits);
    #endif
}


template <class T>
class bitmap_t {
public:
    static constexpr unsigned capacity = sizeof(T) * 8;

    bitmap_t()                                    : bitmap_t(0) {}
    explicit bitmap_t(T bits)                     : _bits(bits) {}
    explicit operator T () const                  { return _bits; }

    unsigned bit_count() const                    { return popcount(_bits); }
    bool empty() const                            { return _bits == 0; }
    bool contains_bit(unsigned bit_no) const      { return (_bits & mask(bit_no)) != 0; }
    unsigned index_of_bit(unsigned bit_no) const  { return popcount( _bits & (mask(bit_no) - 1) ); }
    void add_bit(unsigned bit_no)                 { _bits |= mask(bit_no); }
    void remove_bit(unsigned bit_no)              { _bits &= ~mask(bit_no); }

private:
    T _bits;

    static T mask(unsigned bit_no)                { return T(1) << bit_no; }
};


template <class Rep>
inline bitmap_t<Rep> as_bitmap(Rep bits)          { return bitmap_t<Rep>(bits); }

}
