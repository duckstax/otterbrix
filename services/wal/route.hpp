#pragma once
#include <core/handler_by_id.hpp>

namespace services::wal {

    enum class route : uint64_t {
        create,

        load,
        load_finish,

        create_database,
        drop_database,
        create_collection,
        drop_collection,

        insert_one,
        insert_many,
        delete_one,
        delete_many,
        update_one,
        update_many,
        create_index,

        success,
    };

    constexpr uint64_t handler_id(route type) { return handler_id(group_id_t::wal, type); }

} // namespace services::wal
