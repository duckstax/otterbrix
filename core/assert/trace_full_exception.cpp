#include "trace_full_exception.hpp"

#include <boost/stacktrace/stacktrace.hpp>

#include <fmt/compile.h>
#include <fmt/format.h>

namespace core {

    struct trace_full_exception_base::impl final {
        memory_buffer_t message_buffer_;
        boost::stacktrace::stacktrace stacktrace_;

        impl(memory_buffer_t message_buffer, boost::stacktrace::stacktrace stacktrace)
            : message_buffer_(std::move(message_buffer))
            , stacktrace_(std::move(stacktrace)) {}

        impl() = default;
    };

    trace_full_exception_base::trace_full_exception_base(trace_full_exception_base&& other) noexcept
        : pimpl_(new impl(std::move(other.pimpl_->message_buffer_), std::move(other.pimpl_->stacktrace_))) {
        ensure_null_terminated();
    }

    trace_full_exception_base::~trace_full_exception_base() {
        delete pimpl_;
    }

    void trace_full_exception_base::ensure_null_terminated() {
        pimpl_->message_buffer_.reserve(pimpl_->message_buffer_.size() + 1);
        pimpl_->message_buffer_[pimpl_->message_buffer_.size()] = '\0';
    }

    const char* trace_full_exception::what() const noexcept {
        return message_buffer().size() != 0 ? message_buffer().data() : std::exception::what();
    }

    const trace_full_exception_base::memory_buffer_t&
    trace_full_exception_base::message_buffer() const noexcept {
        return pimpl_->message_buffer_;
    }

    const boost::stacktrace::stacktrace& trace_full_exception_base::trace() const noexcept {
        return pimpl_->stacktrace_;
    }

    trace_full_exception_base::trace_full_exception_base() = default;

    trace_full_exception_base::trace_full_exception_base(std::string_view what) {
        fmt::format_to(std::back_inserter(pimpl_->message_buffer_), FMT_COMPILE("{}"), what);
        ensure_null_terminated();
    }

    trace_full_exception_base::memory_buffer_t& trace_full_exception_base::get_message_buffer() {
        return pimpl_->message_buffer_;
    }

} // namespace core