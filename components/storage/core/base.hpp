#pragma once

#ifndef __has_attribute
    #define __has_attribute(x) 0
#endif

#ifndef __has_builtin
    #define __has_builtin(x) 0
#endif

#ifndef __has_feature
    #define __has_feature(x) 0
#endif

#ifndef __has_extension
    #define __has_extension(x) 0
#endif


#if defined(__clang__) || defined(__GNUC__)
    #define RETURNS_NONNULL                 __attribute__((returns_nonnull))
    #define MUST_USE_RESULT                 __attribute__((warn_unused_result))

    #define _usually_true(VAL)               __builtin_expect(VAL, true)
    #define _usually_false(VAL)              __builtin_expect(VAL, false)
#else
    #define RETURNS_NONNULL
    #define MUST_USE_RESULT

    #define _usually_true(VAL)               (VAL)
    #define _usually_false(VAL)              (VAL)
#endif

#ifdef __clang__
    #define NONNULL                     __attribute__((nonnull))
#else
    #define NONNULL
#endif

#if defined(__GNUC__) || __has_attribute(__pure__)
    #define PURE                      __attribute__((__pure__))
#else
    #define PURE
#endif

#if defined(__GNUC__) || __has_attribute(__const__)
    #define CONST                     __attribute__((__const__))
#else
    #define CONST
#endif

#if __has_attribute(nodebug)
    #define STEPOVER __attribute((nodebug))
#else
    #define STEPOVER
#endif
