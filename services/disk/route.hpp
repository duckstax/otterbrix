#pragma once

namespace services::disk {
    enum class route : uint64_t {
        create_agent,

        read_databases,
        append_database,
        remove_database,

        read_collections,
        append_collection,
        remove_collection,

        read_documents,
        write_documents,
        remove_documents,

        flush,
        fix_wal_id,

        read_databases_finish,
        read_collections_finish,
        read_documents_finish,
    };
} // namespace services::disk