#pragma once
#include <core/handler_by_id.hpp>

namespace services::wal {

    enum class route : uint64_t {
        create,

        insert_one,
        insert_many,

        success,
    };

    inline uint64_t handler_id(route type) {
        return handler_id(group_id_t::wal, type);
    }

} // namespace services::wal
