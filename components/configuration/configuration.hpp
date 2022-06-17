#pragma once

#include <boost/filesystem.hpp>

#include <components/log/log.hpp>

namespace components {

    enum class sandbox_mode_t : std::uint8_t {
        none = 0,
        script = 1,
        jupyter_kernel = 2,
        jupyter_engine = 3,
    };

    enum class operating_mode : std::uint8_t {
        master,
        worker
    };

    struct python_sandbox_configuration final {
        python_sandbox_configuration() {
            mode_ = sandbox_mode_t::none;
        }
        boost::filesystem::path jupyter_connection_path_;
        boost::filesystem::path script_path_;
        sandbox_mode_t mode_;
    };

    struct configuration final {
        configuration() = default;
        python_sandbox_configuration python_configuration_;
        operating_mode operating_mode_;
        unsigned short int port_http_;
    };

    struct config_log final {
        boost::filesystem::path path {boost::filesystem::current_path() / "log"};
        log_t::level level {log_t::level::trace};
    };

    struct config_wal final {
        boost::filesystem::path path {boost::filesystem::current_path() / "wal"};
    };

    struct config_disk final {
        boost::filesystem::path path {boost::filesystem::current_path() / "disk"};
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

} // namespace rocketjoe