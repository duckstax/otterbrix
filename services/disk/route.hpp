#pragma once

#include <core/handler_by_id.hpp>

namespace services::disk {
    enum class route : uint64_t {
        create_agent,

        load,
        load_indexes,
        load_finish,

        append_database,
        remove_database,

        append_collection,
        remove_collection,

        write_documents,
        remove_documents,

        flush,
        fix_wal_id
    };

    constexpr uint64_t handler_id(route type) {
        return handler_id(group_id_t::disk, type);
    }
} // namespace services::disk