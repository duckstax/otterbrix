#pragma once

namespace services::storage {

    namespace manager_database {
        static constexpr auto create_database = "create";
    }

    namespace database {
        static constexpr auto create_collection = "create";
    }

    namespace collection {
        static constexpr auto select = "select";
        static constexpr auto insert = "insert";
        static constexpr auto erase = "erase";
        static constexpr auto search = "search";
        static constexpr auto find = "find";
        static constexpr auto size = "size";
        static constexpr auto close_cursor = "close_cursor";
    } // namespace collection

} // namespace services::storage
