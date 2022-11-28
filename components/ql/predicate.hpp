#pragma once

#include "cstdint"

namespace components::ql {
    enum class condition_type : std::uint8_t {
        novalid,
        eq,
        ne,
        gt,
        lt,
        gte,
        lte,
        regex,
        any,
        all,
        union_and,
        union_or,
        union_not
    };
}