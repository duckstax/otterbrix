#pragma once

#include <actor-zeta.hpp>

#include <boost/filesystem.hpp>
#include <log/log.hpp>

#include <core/excutor.hpp>
#include <core/spinlock/spinlock.hpp>
#include <configuration/configuration.hpp>
#include <components/session/session.hpp>
#include <components/protocol/protocol.hpp>

#include "base.hpp"

namespace services::wal {

    class manager_wal_replicate_t final : public actor_zeta::cooperative_supervisor<manager_wal_replicate_t> {
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

        manager_wal_replicate_t(actor_zeta::detail::pmr::memory_resource*, actor_zeta::scheduler_raw, configuration::config_wal, log_t&);
        void create_wal_worker();
        void load(session_id_t& session, services::wal::id_t wal_id);
        void create_database(session_id_t& session, components::protocol::create_database_t& data);
        void drop_database(session_id_t& session, components::protocol::drop_database_t& data);
        void create_collection(session_id_t& session, components::protocol::create_collection_t& data);
        void drop_collection(session_id_t& session, components::protocol::drop_collection_t& data);
        void insert_one(session_id_t& session, insert_one_t& data);
        void insert_many(session_id_t& session, insert_many_t& data);
        void delete_one(session_id_t& session, delete_one_t& data);
        void delete_many(session_id_t& session, delete_many_t& data);
        void update_one(session_id_t& session, update_one_t& data);
        void update_many(session_id_t& session, update_many_t& data);

        /// NOTE:  sync behold non thread-safety!
        void set_manager_disk(actor_zeta::address_t disk){
            manager_disk_ = disk;
        }

        void set_manager_dispatcher(actor_zeta::address_t dispatcher){
            manager_disk_ = dispatcher;
        }

    protected:
        auto scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* override;
        //NOTE: behold thread-safety!
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void override;

    private:
        spin_lock lock_;
        actor_zeta::address_t manager_disk_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t manager_dispatcher_ = actor_zeta::address_t::empty_address();
        configuration::config_wal config_;
        log_t log_;
        actor_zeta::scheduler_raw e_;
        std::unordered_map<std::string, actor_zeta::address_t> dispatcher_to_address_book_;
        std::vector<actor_zeta::address_t> dispathers_;
    };

    using manager_wal_ptr = std::unique_ptr<manager_wal_replicate_t>;

} //namespace services::wal
