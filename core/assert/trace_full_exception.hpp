#pragma once

#include <exception>
#include <string>
#include <type_traits>

#include <boost/stacktrace/stacktrace_fwd.hpp>
#include <fmt/format.h>

namespace core {

    class trace_full_exception_base {
    public:
        static constexpr size_t inline_buffer_size = 100;
        using memory_buffer_t = fmt::basic_memory_buffer<char, inline_buffer_size>;

        virtual ~trace_full_exception_base();
        trace_full_exception_base(trace_full_exception_base&&) noexcept;

        const memory_buffer_t& message_buffer() const noexcept;
        const boost::stacktrace::basic_stacktrace<>& trace() const noexcept;

        template<typename Exception, typename T>
        friend typename std::enable_if<
            std::is_base_of<trace_full_exception_base, typename std::remove_reference<Exception>::type>::value,
            Exception&&>::type
        operator<<(Exception&& ex, const T& data) {
            fmt::format_to(std::back_inserter(ex.get_message_buffer()), "{}", data);
            ex.ensure_null_terminated();
            return std::forward<Exception>(ex);
        }

    protected:
        trace_full_exception_base();
        explicit trace_full_exception_base(std::string_view what);

    private:
        void ensure_null_terminated();
        memory_buffer_t& get_message_buffer();

        struct impl;
        impl* pimpl_;
    };

    class trace_full_exception
        : public std::exception
        , public trace_full_exception_base {
    public:
        trace_full_exception() = default;
        explicit trace_full_exception(std::string_view what)
            : trace_full_exception_base(what) {}
        const char* what() const noexcept override;
    };
} // namespace core
