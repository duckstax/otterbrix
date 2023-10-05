#pragma once

#include <actor-zeta.hpp>

#include <boost/filesystem.hpp>
#include <log/log.hpp>

#include <core/excutor.hpp>
#include <core/spinlock/spinlock.hpp>
#include <configuration/configuration.hpp>
#include <components/session/session.hpp>
#include <components/ql/statements.hpp>

#include "base.hpp"
#include "wal.hpp"

namespace services::wal {

    class base_manager_wal_replicate_t : public actor_zeta::cooperative_supervisor<base_manager_wal_replicate_t> {
    public:
        auto make_type() const noexcept -> const char* const;
        auto make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t*;
    protected:
        base_manager_wal_replicate_t(std::pmr::memory_resource* mr, actor_zeta::scheduler_raw scheduler);

    private:
        actor_zeta::scheduler_raw e_;
    };

    using manager_wal_ptr = std::unique_ptr<base_manager_wal_replicate_t>;


    class manager_wal_replicate_t final : public base_manager_wal_replicate_t {
        using session_id_t = components::session::session_id_t;
    public:
        using address_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t>;

        enum class unpack_rules : uint64_t {
            manager_disk = 0,
            manager_dispatcher = 1
        };

        void sync(address_pack& pack) {
            manager_disk_ = std::get<static_cast<uint64_t>(unpack_rules::manager_disk)>(pack);
            manager_dispatcher_ = std::get<static_cast<uint64_t>(unpack_rules::manager_dispatcher)>(pack);
        }

        manager_wal_replicate_t(std::pmr::memory_resource*, actor_zeta::scheduler_raw, configuration::config_wal, log_t&);
        ~manager_wal_replicate_t() final;
        void create_wal_worker();
        void load(session_id_t& session, services::wal::id_t wal_id);
        void create_database(session_id_t& session, components::ql::create_database_t& data);
        void drop_database(session_id_t& session, components::ql::drop_database_t& data);
        void create_collection(session_id_t& session, components::ql::create_collection_t& data);
        void drop_collection(session_id_t& session, components::ql::drop_collection_t& data);
        void insert_one(session_id_t& session, components::ql::insert_one_t& data);
        void insert_many(session_id_t& session, components::ql::insert_many_t& data);
        void delete_one(session_id_t& session, components::ql::delete_one_t& data);
        void delete_many(session_id_t& session, components::ql::delete_many_t& data);
        void update_one(session_id_t& session, components::ql::update_one_t& data);
        void update_many(session_id_t& session, components::ql::update_many_t& data);
        void create_index(session_id_t& session, components::ql::create_index_t& data);

        actor_zeta::behavior_t behavior();

    private:
        actor_zeta::behavior_t create_;
        actor_zeta::behavior_t load_;
        actor_zeta::behavior_t create_database_;
        actor_zeta::behavior_t drop_database_;
        actor_zeta::behavior_t create_collection_;
        actor_zeta::behavior_t drop_collection_;
        actor_zeta::behavior_t insert_one_;
        actor_zeta::behavior_t insert_many_;
        actor_zeta::behavior_t delete_one_;
        actor_zeta::behavior_t delete_many_;
        actor_zeta::behavior_t update_one_;
        actor_zeta::behavior_t update_many_;
        actor_zeta::behavior_t core_sync_;
        actor_zeta::behavior_t create_index_;

        actor_zeta::address_t manager_disk_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t manager_dispatcher_ = actor_zeta::address_t::empty_address();
        configuration::config_wal config_;
        log_t log_;
        std::unordered_map<std::string, actor_zeta::address_t> dispatcher_to_address_book_;
        std::vector<wal_replicate_ptr> dispatchers_;
        spin_lock lock_;
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void final;
    };


    class manager_wal_replicate_empty_t final : public base_manager_wal_replicate_t {
        using session_id_t = components::session::session_id_t;
        using address_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t>;
    public:
        manager_wal_replicate_empty_t(std::pmr::memory_resource*, actor_zeta::scheduler_raw, log_t&);
        actor_zeta::behavior_t behavior();
    private:
        auto enqueue_impl(actor_zeta::message_ptr, actor_zeta::execution_unit*) -> void final;
    };

} //namespace services::wal
