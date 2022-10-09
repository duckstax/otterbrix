#pragma once

#include <cstdint>

namespace components::ql::aggregate {

    enum class aggregate_types : int16_t {
        invalid = 1,
        count, ///group + project
        group,
        limit,
        match,
        merge,
        out,
        project,
        skip,
        sort,
        unset,
        unwind,
        finish
    };
}