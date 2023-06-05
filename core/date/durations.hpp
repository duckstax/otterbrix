#pragma once

#include <chrono>

namespace core::date {

    using duration_day = std::chrono::duration<int32_t, std::ratio<3600 * 24>>; //C++ 20
    using duration_h = std::chrono::duration<int32_t, std::chrono::hours::period>;
    using duration_m = std::chrono::duration<int32_t, std::chrono::minutes::period>;
    using duration_s = std::chrono::duration<int64_t, std::chrono::seconds::period>;
    using duration_ms = std::chrono::duration<int64_t, std::chrono::milliseconds::period>;
    using duration_us = std::chrono::duration<int64_t, std::chrono::microseconds::period>;
    using duration_ns = std::chrono::duration<int64_t, std::chrono::nanoseconds::period>;

    static_assert(sizeof(duration_day) == sizeof(typename duration_day::rep), "");
    static_assert(sizeof(duration_h) == sizeof(typename duration_h::rep), "");
    static_assert(sizeof(duration_m) == sizeof(typename duration_m::rep), "");
    static_assert(sizeof(duration_s) == sizeof(typename duration_s::rep), "");
    static_assert(sizeof(duration_ms) == sizeof(typename duration_ms::rep), "");
    static_assert(sizeof(duration_us) == sizeof(typename duration_us::rep), "");
    static_assert(sizeof(duration_ns) == sizeof(typename duration_ns::rep), "");

} // namespace core::date
