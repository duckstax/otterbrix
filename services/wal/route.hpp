#pragma once

namespace services::wal {
    enum class route : uint64_t {
        create,

        insert_one,
        insert_many,

        success,
    };
} // namespace services::wal
