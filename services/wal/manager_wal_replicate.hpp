#pragma once

#include <actor-zeta.hpp>

#include <RocketJoe/core/excutor.hpp>
#include <boost/filesystem.hpp>
#include <log/log.hpp>

#include <components/session/session.hpp>
#include <components/protocol/insert_one.hpp>
#include <components/protocol/insert_many.hpp>

namespace services::wal {

    class manager_wal_replicate_t final : public actor_zeta::cooperative_supervisor<manager_wal_replicate_t> {
        using session_id_t = components::session::session_id_t;

    public:
        manager_wal_replicate_t(actor_zeta::detail::pmr::memory_resource*,boost::filesystem::path, log_t& log, size_t num_workers, size_t max_throughput);
        void creat_wal_worker();
        void insert_one(session_id_t& session, insert_one_t& data);
        void insert_many(session_id_t& session, insert_many_t& data);

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
        actor_zeta::address_t manager_disk_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t manager_dispatcher_ = actor_zeta::address_t::empty_address();
        boost::filesystem::path path_;
        log_t log_;
        actor_zeta::scheduler_ptr e_;
        std::unordered_map<std::string, actor_zeta::address_t> dispatcher_to_address_book_;
        std::vector<actor_zeta::address_t> dispathers_;
    };

    using manager_wal_ptr = std::unique_ptr<manager_wal_replicate_t>;

} //namespace services::wal
