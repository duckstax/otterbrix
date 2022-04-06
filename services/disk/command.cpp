#include "command.hpp"

namespace services::disk {

    void append_command(command_storage_t &storage, const components::session::session_id_t &session, const command_t &command) {
        auto it = storage.find(session);
        if (it != storage.end()) {
            it->second.push_back(command);
        } else {
            std::vector<command_t> commands = {command};
            storage.emplace(session, commands);
        }
    }

} //namespace services::disk