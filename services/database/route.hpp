#pragma once

namespace services::database::route {

    static constexpr auto create_database = "database::create_database";
    static constexpr auto create_collection = "database::create_collection";
    static constexpr auto drop_collection = "database::drop_collection";

    static constexpr auto create_database_finish = "database::create_database_finish";
    static constexpr auto create_collection_finish = "database::create_collection_finish";
    static constexpr auto drop_collection_finish = "database::drop_collection_finish";

} // services::database::route