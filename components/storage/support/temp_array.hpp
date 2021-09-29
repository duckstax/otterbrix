#pragma once

#include <stdlib.h>
#include <memory>

#ifdef _MSC_VER

#define _temp_array(NAME, TYPE, SIZE) \
    std::unique_ptr<TYPE, decltype(_freea)*> NAME##_ptr((TYPE *)_malloca((SIZE) * sizeof(TYPE)), _freea);\
    TYPE* NAME = NAME##_ptr.get()

#else

template <class T>
struct temp_array_t
{
    temp_array_t(size_t n)
        : on_heap(n * sizeof(T) >= 1024)
        , array( on_heap ? new T[n] : nullptr)
    {}

    ~temp_array_t() {
        if (on_heap)
            delete[] array;
    }

    operator T* () { return array; }

    template <class U>
    explicit operator U* () { return reinterpret_cast<U*>(array); }

    bool const on_heap;
    T* array;
};


#define _temp_array(NAME, TYPE, SIZE) \
    temp_array_t<TYPE> NAME(SIZE); \
    if (!NAME.on_heap && (SIZE) > 0) NAME.array = static_cast<TYPE*>(alloca((SIZE)*sizeof(TYPE)));

#endif
