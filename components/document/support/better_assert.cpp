#include "better_assert.hpp"
#include <stdio.h>
#include <string.h>

#ifdef _MSC_VER
#include "asprintf.h"
#endif

namespace document {

    static const char* filename(const char* file) {
#ifndef __FILE_NAME__
        const char* slash = strrchr(file, '/');
        if (!slash)
            slash = strrchr(file, '\\');
        if (slash)
            file = slash + 1;
#endif
        return file;
    }

    static const char* log(const char* format, const char* cond, const char* fn, const char* file, int line) {
        char* msg;
        if (asprintf(&msg, format, cond, (fn ? fn : ""), filename(file), line) > 0) {
            fprintf(stderr, "%s\n", msg);
            return msg;
        } else {
            fprintf(stderr, "%s\n", format);
            return format;
        }
    }

#ifdef __cpp_exceptions
    void _assert_failed(const char* cond, const char* fn, const char* file, int line) {
        throw assertion_failure(log("FAILED ASSERTION `%s` in %s (at %s line %d)", cond, fn, file, line));
    }

    void _precondition_failed(const char* cond, const char* fn, const char* file, int line) {
        throw std::invalid_argument(
            log("FAILED PRECONDITION: `%s` not true when calling %s (at %s line %d)", cond, fn, file, line));
    }

    void _postcondition_failed(const char* cond, const char* fn, const char* file, int line) {
        throw assertion_failure(
            log("FAILED POSTCONDITION: `%s` not true at end of %s (at %s line %d)", cond, fn, file, line));
    }

    assertion_failure::assertion_failure(const char* what)
        : logic_error(what) {}

    [[noreturn]] NOINLINE void _assert_failed_nox(const char* cond, const char* fn, const char* file, int line);
    [[noreturn]] NOINLINE void _precondition_failed_nox(const char* cond, const char* fn, const char* file, int line);
    [[noreturn]] NOINLINE void _postcondition_failed_nox(const char* cond, const char* fn, const char* file, int line);
#endif

    void _assert_failed_nox(const char* cond, const char* fn, const char* file, int line) {
        log("\n***FATAL: FAILED ASSERTION `%s` in %s (at %s line %d)", cond, fn, file, line);
        std::terminate();
    }

    void _precondition_failed_nox(const char* cond, const char* fn, const char* file, int line) {
        log("\n***FATAL: FAILED PRECONDITION: `%s` not true when calling %s (at %s line %d)", cond, fn, file, line);
        std::terminate();
    }

    void _postcondition_failed_nox(const char* cond, const char* fn, const char* file, int line) {
        log("***FATAL: FAILED POSTCONDITION: `%s` not true at end of %s (at %s line %d)", cond, fn, file, line);
        std::terminate();
    }

} // namespace document
