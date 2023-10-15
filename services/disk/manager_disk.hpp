#pragma once

#include <core/excutor.hpp>
#include <core/file/file.hpp>
#include <configuration/configuration.hpp>
#include <components/log/log.hpp>
#include "agent_disk.hpp"
#include "index_agent_disk.hpp"

namespace services::disk {

    using session_id_t = ::components::session::session_id_t;

    class manager_disk_t;
    using manager_disk_ptr = std::unique_ptr<manager_disk_t,actor_zeta::deleter>;

    class manager_disk_t final: public actor_zeta::cooperative_supervisor<manager_disk_t> {
    public:
        using address_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t>;

        enum class unpack_rules : uint64_t {
            manager_wal = 0,
        };

        void sync(address_pack& pack) {
            manager_wal_ = std::get<static_cast<uint64_t>(unpack_rules::manager_wal)>(pack);
        }

        manager_disk_t(std::pmr::memory_resource*, actor_zeta::scheduler_raw, configuration::config_disk config, log_t& log);
        ~manager_disk_t();

        void create_agent();

        auto load(session_id_t& session) -> void;
        auto load_indexes(session_id_t& session) -> void;

        auto append_database(session_id_t& session, const database_name_t& database) -> void;
        auto remove_database(session_id_t& session, const database_name_t& database) -> void;

        auto append_collection(session_id_t& session, const database_name_t& database, const collection_name_t& collection) -> void;
        auto remove_collection(session_id_t& session, const database_name_t& database, const collection_name_t& collection) -> void;

        auto write_documents(session_id_t& session, const database_name_t& database, const collection_name_t& collection, const std::pmr::vector<document_ptr>& documents) -> void;
        auto remove_documents(session_id_t& session, const database_name_t& database, const collection_name_t& collection, const std::pmr::vector<document_id_t>& documents) -> void;

        auto flush(session_id_t& session, wal::id_t wal_id) -> void;

        void create_index_agent(session_id_t& session, const components::ql::create_index_t &index);
        void drop_index_agent(session_id_t& session, const index_name_t &index_name);
        void drop_index_agent_success(session_id_t& session);

        auto make_type() const noexcept -> const char* const {
            return "manager_disk";
        }
        actor_zeta::behavior_t behavior();
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void final;
        auto make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t*;
    private:
        actor_zeta::behavior_t core_sync_;
        actor_zeta::behavior_t create_agent_;
        actor_zeta::behavior_t load_;
        actor_zeta::behavior_t load_indexes_;
        actor_zeta::behavior_t append_database_;
        actor_zeta::behavior_t remove_database_;
        actor_zeta::behavior_t append_collection_;
        actor_zeta::behavior_t remove_collection_;
        actor_zeta::behavior_t write_documents_;
        actor_zeta::behavior_t remove_documents_;
        actor_zeta::behavior_t flush_;
        actor_zeta::behavior_t create_;
        actor_zeta::behavior_t drop_;
        actor_zeta::behavior_t success_;

        actor_zeta::scheduler_raw e_;

        actor_zeta::address_t manager_wal_ = actor_zeta::address_t::empty_address();
        log_t log_;
        configuration::config_disk config_;
        std::vector<agent_disk_ptr> agents_;
        index_agent_disk_storage_t index_agents_;
        command_storage_t commands_;
        file_ptr metafile_indexes_;
        session_id_t load_session_;

        struct removed_index_t {
            std::size_t size;
            command_t command;
            actor_zeta::address_t sender;
        };
        std::pmr::unordered_map<session_id_t, removed_index_t> removed_indexes_;

        auto agent() -> actor_zeta::address_t;
        void write_index_private(const components::ql::create_index_t &index);
        void load_indexes_private(session_id_t& session, const actor_zeta::address_t& dispatcher);
        std::vector<components::ql::create_index_t> read_indexes_private(const collection_name_t& collection_name) const;
        std::vector<components::ql::create_index_t> read_indexes_private() const;
        void remove_index_private(const index_name_t& index_name);
        void remove_all_indexes_from_collection_private(const collection_name_t& collection_name);
    };


    class manager_disk_empty_t final: public actor_zeta::cooperative_supervisor<manager_disk_empty_t> {
    public:
        manager_disk_empty_t(std::pmr::memory_resource*, actor_zeta::scheduler_raw);

        auto load(session_id_t& session) -> void;
        void create_index_agent(session_id_t& session, const collection_name_t&, const index_name_t&, components::ql::index_compare);
        auto make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t*;
        auto make_type() const noexcept -> const char* const;
        actor_zeta::behavior_t behavior();
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void final;
    private:
        actor_zeta::scheduler_raw e_;
    };

} //namespace services::disk