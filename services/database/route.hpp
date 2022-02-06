#pragma once

namespace services::storage {

    namespace manager_database {
        static constexpr auto create_database = "create_database";
    }

    namespace database {
        static constexpr auto create_collection = "create_collection";
        static constexpr auto drop_collection = "drop_collection";
    } // namespace database

} // namespace services::storage
