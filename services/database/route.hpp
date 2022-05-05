#pragma once

namespace services::database {
    enum class route : uint64_t {
        create_database,
        create_collection,
        drop_collection,
        create_database_finish,
        create_collection_finish,
        drop_collection_finish
    };
} // namespace services::database