#pragma once

namespace services::disk::route {

    static constexpr auto create_agent = "disk::create_agent";

    static constexpr auto append_database = "disk::append_database";
    static constexpr auto remove_database = "disk::remove_database";

    static constexpr auto append_collection = "disk::append_collection";
    static constexpr auto remove_collection = "disk::remove_collection";

    static constexpr auto write_documents = "disk::write_documents";
    static constexpr auto remove_documents = "disk::remove_documents";

    static constexpr auto flush = "disk::flush";
    static constexpr auto fix_wal_id = "disk::fix_wal_id";

    static constexpr auto load = "disk::load";
    static constexpr auto load_databases = "disk::load_databases";
    static constexpr auto load_collections = "disk::load_collections";
    static constexpr auto load_documents = "disk::load_documents";
    static constexpr auto load_finish = "disk::load_finish";

} // namespace services::disk::route