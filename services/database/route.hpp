#pragma once
#include <core/handler_by_id.hpp>
namespace services::database {
    enum class route : uint64_t {
        create_database,
        create_collection,
        drop_collection,
        create_database_finish,
        create_collection_finish,
        drop_collection_finish
    };

    inline uint64_t handler_id(route type) {
        return handler_id(group_id_t::database, type);
    }
} // namespace services::database