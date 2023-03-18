#pragma once

#include <core/excutor.hpp>
#include <configuration/configuration.hpp>
#include <components/log/log.hpp>
#include "agent_disk.hpp"
#include "index_agent_disk.hpp"

namespace services::disk {

    using session_id_t = ::components::session::session_id_t;


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
        ~manager_disk_t();

        void create_agent();

        auto load(session_id_t& session) -> void;

        auto append_database(session_id_t& session, const database_name_t& database) -> void;
        auto remove_database(session_id_t& session, const database_name_t& database) -> void;

        auto append_collection(session_id_t& session, const database_name_t& database, const collection_name_t& collection) -> void;
        auto remove_collection(session_id_t& session, const database_name_t& database, const collection_name_t& collection) -> void;

        auto write_documents(session_id_t& session, const database_name_t& database, const collection_name_t& collection, const std::pmr::vector<document_ptr>& documents) -> void;
        auto remove_documents(session_id_t& session, const database_name_t& database, const collection_name_t& collection, const std::pmr::vector<document_id_t>& documents) -> void;

        auto flush(session_id_t& session, wal::id_t wal_id) -> void;

        void create_index_agent(session_id_t& session, const collection_name_t &collection_name, const index_name_t &index_name, index_disk_t::compare compare_type);
        void drop_index_agent(session_id_t& session, const index_name_t &index_name);
        void drop_index_agent_success(session_id_t& session);

    private:
        actor_zeta::address_t manager_wal_ = actor_zeta::address_t::empty_address();
        log_t log_;
        configuration::config_disk config_;
        std::vector<agent_disk_ptr> agents_;
        index_agent_disk_storage_t index_agents_;
        command_storage_t commands_;

        auto agent() -> actor_zeta::address_t;
    };


    class manager_disk_empty_t final : public base_manager_disk_t {
    public:
        manager_disk_empty_t(actor_zeta::detail::pmr::memory_resource*, actor_zeta::scheduler_raw);

        auto load(session_id_t& session) -> void;
        void create_index_agent(session_id_t& session, const collection_name_t&, const index_name_t&, index_disk_t::compare);

        template<class ...Args>
        auto nothing(Args&&...) -> void {}
    };

} //namespace services::disk