#include "exception.hpp"
#include "platform_compat.hpp"
#include <errno.h>
#include <stdarg.h>
#include <memory>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <string.h>

namespace document {

static const char* const kErrorNames[] = {
    "",
    "Memory error",
    "Array/iterator index out of range",
    "Invalid input data",
    "Encoder error",
    "JSON error",
    "Unknown value; data may be corrupt",
    "path_t syntax error",
    "Internal library error",
    "key not found",
    "Incorrect use of persistent shared keys",
    "POSIX error",
    "Unsupported operation"
};


exception_t::exception_t(error_code code_, const std::string &what)
    : std::runtime_error(what)
    , code(code_)
{}

void exception_t::_throw(error_code code, const char *what, ...) {
    std::string message = kErrorNames[static_cast<int>(code)];
    if (what) {
        va_list args;
        va_start(args, what);
        char *msg;
        int len = vasprintf(&msg, what, args);
        va_end(args);
        if (len >= 0) {
            message += std::string(": ") + msg;
            free(msg);
        }
    }
    throw exception_t(code, message);
}

void exception_t::_throw_errno(const char *what, ...) {
    va_list args;
    va_start(args, what);
    char *msg;
    int len = vasprintf(&msg, what, args);
    va_end(args);
    std::string message;
    if (len >= 0) {
        message = std::string(msg) + ": " + strerror(errno);
        free(msg);
    }
    throw exception_t(error_code::posix_error, message);
}

error_code exception_t::get_code(const std::exception &x) noexcept {
    auto fleecex = dynamic_cast<const exception_t*>(&x);
    if (fleecex)
        return fleecex->code;
    else if (nullptr != dynamic_cast<const std::bad_alloc*>(&x))
        return error_code::memory_error;
    else
        return error_code::internal_error;
}

}
