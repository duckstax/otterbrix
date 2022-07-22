#pragma once

#include <stdexcept>
#include <memory>
#include <components/document/core/base.hpp>

namespace document {

enum class error_code {
    no_error,
    memory_error,
    out_of_range,
    invalid_data,
    encode_error,
    json_error,
    unknown_value,
    path_syntax_error,
    internal_error,
    not_found,
    shared_keys_state_error,
    posix_error,
    unsupported
};


class exception_t : public std::runtime_error {
public:
    exception_t(error_code code_, const std::string &what);

    [[noreturn]] static void _throw(error_code code, const char *what, ...);
    [[noreturn]] static void _throw_errno(const char *what, ...);

    static error_code get_code(const std::exception&) noexcept;

    const error_code code;
};

#define _throw_if(BAD, ERROR, MESSAGE) \
    if (_usually_true(!(BAD))) ; else document::exception_t::_throw(ERROR, MESSAGE)

}
