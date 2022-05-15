#include "manager_wal_replicate.hpp"

#include "wal.hpp"
#include "route.hpp"

#include <core/system_command.hpp>

namespace services::wal {

    manager_wal_replicate_t::manager_wal_replicate_t(actor_zeta::detail::pmr::memory_resource*mr,actor_zeta::scheduler_raw scheduler,boost::filesystem::path path, log_t& log, size_t num_workers, size_t max_throughput)
        : actor_zeta::cooperative_supervisor<manager_wal_replicate_t>(mr,"manager_wal")
        , path_(path)
        , log_(log.clone())
        , e_(scheduler) {
        trace(log_, "manager_wal_replicate_t num_workers : {} , max_throughput: {}", num_workers, max_throughput);
        add_handler(handler_id(route::create), &manager_wal_replicate_t::creat_wal_worker);
        add_handler(handler_id(route::insert_one), &manager_wal_replicate_t::insert_one);
        add_handler(handler_id(route::insert_many), &manager_wal_replicate_t::insert_many);
        add_handler(core::handler_id(core::route::sync), &manager_wal_replicate_t::sync);
        trace(log_, "manager_wal_replicate_t start thread pool");
        e_->start();
    }

    auto manager_wal_replicate_t::scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* {
        return e_.get();
    }

    auto manager_wal_replicate_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        set_current_message(std::move(msg));
        execute(this,current_message());
    }

    void manager_wal_replicate_t::creat_wal_worker() {
        auto address = spawn_actor<wal_replicate_t>([this](wal_replicate_t*ptr){
            dispathers_.emplace_back(ptr->address());
        },log_, path_);
    }

    void manager_wal_replicate_t::insert_one(session_id_t& session, insert_one_t& data) {
        trace(log_, "manager_wal_replicate_t::insert_one");
        actor_zeta::send(dispathers_[0], address(), route::insert_one, session, current_message()->sender(), std::move(data));
    }

    void manager_wal_replicate_t::insert_many(session_id_t& session, insert_many_t& data) {
        trace(log_, "manager_wal_replicate_t::insert_many");
        actor_zeta::send(dispathers_[0], address(), route::insert_many, session, current_message()->sender(), std::move(data));
    }

} //namespace services::wal
