#include "assert.hpp"

#include <iostream>

#include <boost/stacktrace.hpp>
#include <fmt/format.h>

#include <components/log/log.hpp>

#include "trace_full_exception.hpp"

namespace core::detail {

    class InvariantError : public trace_full_exception {
        using trace_full_exception::trace_full_exception;
    };

    void failed(std::string_view expr, const char* file, unsigned int line,
                const char* function, std::string_view msg) noexcept {
        auto trace = boost::stacktrace::stacktrace();
        auto log = get_logger();
        error(log, "error at {}:{}:{}. assertion '{}' failed{}{}.\n Stacktrace:\n{}\n", file,
              line, (function ? function : ""), expr,
              (msg.empty() ? std::string_view{} : std::string_view{": "}), msg,
              to_string(trace));
        abort();
    }

    void log_and_throw_invariant_error(std::string_view condition, std::string_view message) {
        const std::string err_str = fmt::format("invariant ({}) violation: {}", condition, message);
        auto log = get_logger();
        error(log, err_str);
        throw InvariantError(err_str);
    }

} // namespace core::detail
