#pragma once

#include <core/handler_by_id.hpp>

namespace services::index {

    enum class route : uint64_t
    {
        create,
        drop,
        insert,
        insert_many,
        remove,
        find,
        success,
        success_create,
        success_find,
        error
    };

    constexpr auto handler_id(route type) { return handler_id(group_id_t::index, type); }

} // namespace services::index
