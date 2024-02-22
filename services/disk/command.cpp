#include "command.hpp"
#include "route.hpp"
#include <components/index/disk/route.hpp>

namespace services::disk {

    command_t::command_name_t command_t::name() const {
        return std::visit(
            [](const auto& c) {
                using command_type = std::decay_t<decltype(c)>;
                if constexpr (std::is_same_v<command_type, command_append_database_t>) {
                    return handler_id(route::append_database);
                } else if constexpr (std::is_same_v<command_type, command_remove_database_t>) {
                    return handler_id(route::remove_database);
                } else if constexpr (std::is_same_v<command_type, command_append_collection_t>) {
                    return handler_id(route::append_collection);
                } else if constexpr (std::is_same_v<command_type, command_remove_collection_t>) {
                    return handler_id(route::remove_collection);
                } else if constexpr (std::is_same_v<command_type, command_write_documents_t>) {
                    return handler_id(route::write_documents);
                } else if constexpr (std::is_same_v<command_type, command_remove_documents_t>) {
                    return handler_id(route::remove_documents);
                } else if constexpr (std::is_same_v<command_type, command_drop_index_t>) {
                    return handler_id(index::route::drop);
                }
                static_assert(true, "Not valid command type");
            },
            command_);
    }

    void append_command(command_storage_t& storage,
                        const components::session::session_id_t& session,
                        const command_t& command) {
        auto it = storage.find(session);
        if (it != storage.end()) {
            it->second.push_back(command);
        } else {
            std::vector<command_t> commands = {command};
            storage.emplace(session, commands);
        }
    }

} //namespace services::disk