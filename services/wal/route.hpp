#pragma once

namespace services::wal::route {

    static constexpr auto create = "wal::create";

    static constexpr auto create_database = "wal::create_database";
    static constexpr auto drop_database = "wal::drop_database";
    static constexpr auto create_collection = "wal::create_collection";
    static constexpr auto drop_collection = "wal::drop_collection";

    static constexpr auto insert_one = "wal::insert_one";
    static constexpr auto insert_many = "wal::insert_many";

    static constexpr auto success = "wal::success";

} // namespace services::wal::route
