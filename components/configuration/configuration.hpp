#pragma once

#include <filesystem>

#include <components/log/log.hpp>

namespace configuration {

    struct config_log final {
        std::filesystem::path path {std::filesystem::current_path() / "log"};
        log_t::level level {log_t::level::trace};
    };

    struct config_wal final {
        std::filesystem::path path {std::filesystem::current_path() / "wal"};
        bool on {true};
        bool sync_to_disk {true};
    };

    struct config_disk final {
        std::filesystem::path path {std::filesystem::current_path() / "disk"};
        bool on {true};
    };

    struct config final {
        config_log log;
        config_wal wal;
        config_disk disk;

        static config default_config() {
            return config();
        }
    };

} // namespace ottergon