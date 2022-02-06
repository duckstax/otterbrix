#pragma once

#include "services/storage/route.hpp"

namespace services::dispatcher {

    using namespace services::storage;

    namespace dispatcher {
        static constexpr auto create_collection = "create_collection";
        static constexpr auto create_database = "create_database";
        static constexpr auto select = "select";
        static constexpr auto insert = "insert";
        static constexpr auto erase = "erase";
    } // namespace dispatcher

} // namespace services::dispatcher

enum class Type : char {
    init = 0x00,
    create_database,
    create_collection,
    drop_collection,
    insert_one,
    insert_many,
    delete_one,
    delete_many,
    update_one,
    update_many
};
