#pragma once

#include "goblin-engineer/core.hpp"
#include <excutor.hpp>
#include <log/log.hpp>


using manager = goblin_engineer::basic_manager_service_t<goblin_engineer::base_policy_light>;

class manager_wal_replicate_t final : public manager {
public:
    manager_wal_replicate_t(log_t& log, size_t num_workers, size_t max_throughput)
        : manager("manager_dispatcher")
        , log_(log.clone())
        , e_(new goblin_engineer::shared_work(num_workers, max_throughput), goblin_engineer::detail::thread_pool_deleter()) {
        log_.trace("manager_dispatcher_t::manager_dispatcher_t num_workers : {} , max_throughput: {}", num_workers, max_throughput);

        log_.trace("manager_dispatcher_t start thread pool");
        e_->start();
    }

protected:
    auto executor_impl() noexcept -> goblin_engineer::abstract_executor* {
        return e_.get();
    }

    //NOTE: behold thread-safety!
    auto enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void {
        set_current_message(std::move(msg));
        execute();
    }
    auto add_actor_impl(goblin_engineer::actor a) -> void {
        actor_storage_.emplace_back(std::move(a));
    }
    auto add_supervisor_impl(goblin_engineer::supervisor) -> void {
        log_.error("manager_dispatcher_t::add_supervisor_impl");
    }

private:
    log_t log_;
    goblin_engineer::executor_ptr e_;
    std::vector<goblin_engineer::actor> actor_storage_;
    std::unordered_map<std::string, goblin_engineer::address_t> dispatcher_to_address_book_;
    std::vector<goblin_engineer::address_t> dispathers_;
};

using manager_wr_ptr = goblin_engineer::intrusive_ptr<manager_wal_replicate_t>;