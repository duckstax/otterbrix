#pragma once

#include <core/handler_by_id.hpp>

namespace services::dispatcher {

    enum class route : uint64_t
    {
        create,
        execute_plan,
        execute_plan_finish
    };

    constexpr auto handler_id(route type) { return handler_id(group_id_t::dispatcher, type); }

} // namespace services::dispatcher
