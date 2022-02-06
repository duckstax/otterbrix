#pragma once

namespace services::storage {

    namespace manager_database {
        static constexpr auto create_database = "create_database";
    }

    namespace database {
        static constexpr auto create_collection = "create_collection";
        static constexpr auto drop_collection = "drop_collection";
    }

    namespace collection {
        static constexpr auto insert_one = "insert_one";
        static constexpr auto insert_many = "insert_many";
        static constexpr auto find = "find";
        static constexpr auto find_one = "find_one";
        static constexpr auto delete_one = "delete_one";
        static constexpr auto delete_many = "delete_many";
        static constexpr auto update_one = "update_one";
        static constexpr auto update_many = "update_many";
        static constexpr auto size = "size";
        static constexpr auto close_cursor = "close_cursor";
    } // namespace collection

} // namespace services::storage
