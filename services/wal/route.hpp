#pragma once

namespace services::wal::route {

    static constexpr auto create = "wal::create";

    static constexpr auto insert_one = "wal::insert_one";
    static constexpr auto insert_many = "wal::insert_many";

    static constexpr auto success = "wal::success";

} // namespace services::wal::route
