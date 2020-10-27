#pragma once

#include <ostream>
#include <string>
#include <vector>

#include <boost/process/child.hpp>

namespace components {

    struct ssh_remote_t final {
        std::string host;
        std::uint16_t port;
    };

    auto operator<<(
        std::ostream& stream,
        const ssh_remote_t& remote) -> std::ostream&;

    struct ssh_socket_forward_t final {
        std::string local_host;
        std::uint16_t local_port;
        std::string remote_host;
        std::uint16_t remote_port;
    };

    auto operator<<(
        std::ostream& stream,
        const ssh_socket_forward_t& socket_forward) -> std::ostream&;

    class ssh_forwarder_t final {
    public:
        ssh_forwarder_t(
            ssh_remote_t remote,
            std::vector<ssh_socket_forward_t> socket_forwards);
        ssh_forwarder_t(const ssh_forwarder_t&) = delete;
        auto operator=(const ssh_forwarder_t&) -> ssh_forwarder_t& = delete;

    private:
        boost::process::child ssh_handler_;
    };

} // namespace components
