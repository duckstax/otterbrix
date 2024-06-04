#include "base_spaces.hpp"
#include "route.hpp"
#include <actor-zeta.hpp>
#include <core/system_command.hpp>
#include <memory>
#include <services/disk/manager_disk.hpp>
#include <services/memory_storage/memory_storage.hpp>

namespace otterbrix {

    constexpr static auto name_dispatcher = "dispatcher";

    base_otterbrix_t::base_otterbrix_t(const configuration::config& config)
        : scheduler_(new actor_zeta::shared_work(1, 1000))
        , scheduler_dispather_(new actor_zeta::shared_work(1, 1000)) {
        log_ = initialization_logger("python", config.log.path.c_str());
        log_.set_level(config.log.level);
        trace(log_, "spaces::spaces()");

        ///scheduler_.reset(new actor_zeta::shared_work(1, 1000), actor_zeta::detail::thread_pool_deleter());
        resource = actor_zeta::detail::pmr::get_default_resource();

        trace(log_, "manager_disk start");
        if (config.disk.on) {
            manager_disk_ = actor_zeta::spawn_supervisor<services::disk::manager_disk_t>(resource,
                                                                                         scheduler_.get(),
                                                                                         config.disk,
                                                                                         log_);
        } else {
            manager_disk_ =
                actor_zeta::spawn_supervisor<services::disk::manager_disk_empty_t>(resource, scheduler_.get(), log_);
        }
        trace(log_, "manager_disk finish");

        trace(log_, "memory_storage start");
        memory_storage_ =
            actor_zeta::spawn_supervisor<services::memory_storage_t>(resource, scheduler_.get(), config.wal, log_);
        trace(log_, "memory_storage finish");

        trace(log_, "wrapper_dispatcher start");
        wrapper_dispatcher_ = std::make_unique<wrapper_dispatcher_t>(resource, memory_storage_->address(), log_);
        trace(log_, "wrapper_dispatcher finish");

        actor_zeta::send(manager_disk_,
                         actor_zeta::address_t::empty_address(),
                         core::handler_id(core::route::sync),
                         std::make_tuple(memory_storage_->address()));

        actor_zeta::send(memory_storage_,
                         actor_zeta::address_t::empty_address(),
                         core::handler_id(core::route::sync),
                         std::make_tuple(wrapper_dispatcher_->address(), manager_disk_->address()));

        actor_zeta::send(manager_disk_,
                         actor_zeta::address_t::empty_address(),
                         disk::handler_id(disk::route::create_agent));
        scheduler_dispather_->start();
        scheduler_->start();
        trace(log_, "spaces::spaces() final");
    }

    log_t& base_otterbrix_t::get_log() { return log_; }

    wrapper_dispatcher_t* base_otterbrix_t::dispatcher() { return wrapper_dispatcher_.get(); }

    base_otterbrix_t::~base_otterbrix_t() {
        trace(log_, "delete spaces");
        scheduler_->stop();
        scheduler_dispather_->stop();
    }

} // namespace otterbrix
