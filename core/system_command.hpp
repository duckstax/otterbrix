#pragma once

#include "handler_by_id.hpp"

namespace core {
    enum class route : uint64_t {
        sync = 0,
        load,

        load_finish
    };

    constexpr auto handler_id(route type) {
        return handler_id(group_id_t::system, type);
    }
} // namespace core