#pragma once

#include "command.hpp"
#include "disk.hpp"

#include <core/excutor.hpp>
#include <configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace services::disk {

    using name_t = std::string;
    using session_id_t = ::components::session::session_id_t;

    class base_manager_disk_t;

    class agent_disk_t final : public actor_zeta::basic_async_actor {
    public:
        agent_disk_t(base_manager_disk_t*, const path_t& path_db, const name_t& name, log_t& log);

        auto load(session_id_t& session, actor_zeta::address_t dispatcher) -> void;

        auto append_database(const command_t& command) -> void;
        auto remove_database(const command_t& command) -> void;

        auto append_collection(const command_t& command) -> void;
        auto remove_collection(const command_t& command) -> void;

        auto write_documents(const command_t& command) -> void;
        auto remove_documents(const command_t& command) -> void;

        auto fix_wal_id(wal::id_t wal_id) -> void;

    private:
        log_t log_;
        disk_t disk_;
    };

    using agent_disk_ptr = std::unique_ptr<agent_disk_t>;


    class base_manager_disk_t : public actor_zeta::cooperative_supervisor<base_manager_disk_t> {
    protected:
        base_manager_disk_t(actor_zeta::detail::pmr::memory_resource* mr, actor_zeta::scheduler_raw scheduler);

    private:
        actor_zeta::scheduler_raw e_;

        auto scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* final;
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void final;
    };

    using manager_disk_ptr = std::unique_ptr<base_manager_disk_t>;


    class manager_disk_t final : public base_manager_disk_t {
    public:
        using address_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t>;

        enum class unpack_rules : uint64_t {
            manager_wal = 0,
        };

        void sync(address_pack& pack) {
            manager_wal_ = std::get<static_cast<uint64_t>(unpack_rules::manager_wal)>(pack);
        }

        manager_disk_t(actor_zeta::detail::pmr::memory_resource*, actor_zeta::scheduler_raw, configuration::config_disk config, log_t& log);

        void create_agent();

        auto load(session_id_t& session) -> void;

        auto append_database(session_id_t& session, const database_name_t& database) -> void;
        auto remove_database(session_id_t& session, const database_name_t& database) -> void;

        auto append_collection(session_id_t& session, const database_name_t& database, const collection_name_t& collection) -> void;
        auto remove_collection(session_id_t& session, const database_name_t& database, const collection_name_t& collection) -> void;

        auto write_documents(session_id_t& session, const database_name_t& database, const collection_name_t& collection, const std::vector<document_ptr>& documents) -> void;
        auto remove_documents(session_id_t& session, const database_name_t& database, const collection_name_t& collection, const std::vector<document_id_t>& documents) -> void;

        auto flush(session_id_t& session, wal::id_t wal_id) -> void;

    private:
        actor_zeta::address_t manager_wal_ = actor_zeta::address_t::empty_address();
        log_t log_;
        configuration::config_disk config_;
        std::vector<agent_disk_ptr> agents_;
        command_storage_t commands_;

        auto agent() -> actor_zeta::address_t;
    };


    class manager_disk_empty_t final : public base_manager_disk_t {
    public:
        manager_disk_empty_t(actor_zeta::detail::pmr::memory_resource*, actor_zeta::scheduler_raw);

        auto load(session_id_t& session) -> void;

        template<class ...Args>
        auto nothing(Args&&...args) -> void {}
    };

} //namespace services::disk