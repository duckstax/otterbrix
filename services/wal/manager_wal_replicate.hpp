#pragma once

#include "goblin-engineer/core.hpp"
#include <excutor.hpp>
#include <log/log.hpp>
#include <boost/filesystem.hpp>


using manager = goblin_engineer::basic_manager_service_t<goblin_engineer::base_policy_light>;

class manager_wal_replicate_t final : public manager {
public:
    manager_wal_replicate_t(boost::filesystem::path,log_t& log, size_t num_workers, size_t max_throughput);
    void creat_wal_worker();
protected:
    auto executor_impl() noexcept -> goblin_engineer::abstract_executor* override;
    //NOTE: behold thread-safety!
    auto enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void;
    auto add_actor_impl(goblin_engineer::actor a) -> void override;
    auto add_supervisor_impl(goblin_engineer::supervisor) -> void override;

private:
    boost::filesystem::path path_;
    log_t log_;
    goblin_engineer::executor_ptr e_;
    std::vector<goblin_engineer::actor> actor_storage_;
    std::unordered_map<std::string, goblin_engineer::address_t> dispatcher_to_address_book_;
    std::vector<goblin_engineer::address_t> dispathers_;
};

using manager_wr_ptr = goblin_engineer::intrusive_ptr<manager_wal_replicate_t>;