#pragma once

#include <filesystem>

#include <components/log/log.hpp>

namespace configuration {

    struct config_log final {
        std::string name;
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

    struct config_t final {
        config_log log;
        config_wal wal;
        config_disk disk;

        static config_t default_config() {
            return config_t();
        }
    };

} // namespace ottergon