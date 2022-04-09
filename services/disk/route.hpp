#pragma once

namespace services::disk::route {

    static constexpr auto create_agent = "create_agent";

    static constexpr auto read_databases = "disk::read_databases";
    static constexpr auto append_database = "disk::append_database";
    static constexpr auto remove_database = "disk::remove_database";

    static constexpr auto read_collections = "disk::read_collections";
    static constexpr auto append_collection = "disk::append_collection";
    static constexpr auto remove_collection = "disk::remove_collection";

    static constexpr auto read_documents = "disk::read_documents";
    static constexpr auto write_documents = "disk::write_documents";
    static constexpr auto remove_documents = "disk::remove_documents";

    static constexpr auto flush = "disk::flush";
    static constexpr auto fix_wal_id = "disk::fix_wal_id";

    static constexpr auto read_databases_finish = "disk::read_databases_finish";
    static constexpr auto read_collections_finish = "disk::read_collections_finish";
    static constexpr auto read_documents_finish = "disk::read_documents_finish";

} // namespace services::disk::route