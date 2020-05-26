#pragma once

#include <boost/filesystem.hpp>

namespace rocketjoe {

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

} // namespace rocketjoe