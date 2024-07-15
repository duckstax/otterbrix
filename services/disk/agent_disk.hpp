#pragma once

#include "command.hpp"
#include "disk.hpp"

#include <components/log/log.hpp>
#include <configuration/configuration.hpp>
#include <core/excutor.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace services::disk {

    using name_t = std::string;
    using session_id_t = ::components::session::session_id_t;

    class base_manager_disk_t;

    class agent_disk_t final : public actor_zeta::basic_async_actor {
    public:
        agent_disk_t(base_manager_disk_t*, const path_t& path_db, const name_t& name, log_t& log);
        ~agent_disk_t();

        auto load(session_id_t& session, actor_zeta::address_t dispatcher) -> void;

        auto append_database(const command_t& command) -> void;
        auto remove_database(const command_t& command) -> void;

        auto append_collection(const command_t& command) -> void;
        auto remove_collection(const command_t& command) -> void;

        auto write_documents(const command_t& command) -> void;
        auto remove_documents(const command_t& command) -> void;

        auto fix_wal_id(wal::id_t wal_id) -> void;

    private:
        std::pmr::memory_resource* resource_;
        log_t log_;
        disk_t disk_;
    };

    using agent_disk_ptr = std::unique_ptr<agent_disk_t, std::function<void(agent_disk_t*)>>;

} //namespace services::disk