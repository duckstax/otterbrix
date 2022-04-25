#pragma once

namespace services::wal {

    using id_t = std::uint64_t;
    using atomic_id_t = std::atomic<id_t>;

    inline void next_id(atomic_id_t &id) {
        ++id;
    }

} //namespace services::wal
