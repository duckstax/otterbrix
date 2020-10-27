#include <components/ssh_forwarder/ssh_forwarder.hpp>

#include <sstream>

namespace components {

    auto operator<<(
        std::ostream& stream,
        const ssh_remote_t& remote) -> std::ostream& {
        stream << remote.host
               << ':'
               << remote.port;

        return stream;
    }

    auto operator<<(
        std::ostream& stream,
        const ssh_socket_forward_t& socket_forward) -> std::ostream& {
        stream << socket_forward.local_host
               << ':'
               << socket_forward.local_port
               << ':'
               << socket_forward.remote_host
               << ':'
               << socket_forward.remote_port;

        return stream;
    }

    static auto make_command_line(
        const ssh_remote_t remote,
        const std::vector<ssh_socket_forward_t> socket_forwards) -> std::string {
        std::ostringstream command_line;

        command_line << "ssh -aNnTx";

        for (const auto& socket_forward : socket_forwards) {
            command_line << " -L " << socket_forward;
        }

        command_line << remote;

        return command_line.str();
    }

    ssh_forwarder_t::ssh_forwarder_t(
        ssh_remote_t remote,
        std::vector<ssh_socket_forward_t> socket_forwards)
        : ssh_handler_(make_command_line(
              std::move(remote),
              std::move(socket_forwards))) {
    }

} // namespace components
