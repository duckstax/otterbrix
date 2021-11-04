#pragma once

#include "../core/base.hpp"

#ifdef _MSC_VER
    #define NOINLINE                        __declspec(noinline)
    #define ALWAYS_INLINE                   inline
    #define ASSUME(cond)                    __assume(cond)
	#define LITECORE_UNUSED
    #define __typeof                        decltype

    #define __func__                        __FUNCTION__

    #include <BaseTsd.h>
    typedef SSIZE_T ssize_t;

    #define MAXFLOAT FLT_MAX

    #define __printflike(A, B)

    #define cbl_strdup _strdup
    #define cbl_getcwd _getcwd

    #include <winapifamily.h>

#else
    #define LITECORE_UNUSED                 __attribute__((unused))
    #define NOINLINE                        __attribute((noinline))
    #if __has_attribute(always_inline)
        #define ALWAYS_INLINE               __attribute__((always_inline)) inline
    #else
        #define ALWAYS_INLINE               inline
    #endif

    #if __has_builtin(__builtin_assume)
        #define ASSUME(cond)                __builtin_assume(cond)
    #else
        #define ASSUME(cond)                (void(0))
    #endif

    #ifndef __printflike
    #define __printflike(fmtarg, firstvararg) __attribute__((__format__ (__printf__, fmtarg, firstvararg)))
    #endif

    #define cbl_strdup strdup
    #define cbl_getcwd getcwd

#endif
