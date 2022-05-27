#pragma once

#include <string>
#include <atomic>

namespace services::wal {

    using id_t = std::uint64_t;
    using atomic_id_t = std::atomic<id_t>;

    inline void next_id(atomic_id_t &id) {
        ++id;
    }

    inline id_t id_from_string(const std::string &value) {
        if (value.empty()) {
            return 0;
        }
        return std::strtoull(value.data(), nullptr, 10);
    }

} //namespace services::wal
