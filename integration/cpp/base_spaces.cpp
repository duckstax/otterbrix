#include "base_spaces.hpp"
#include <memory>
#include <actor-zeta.hpp>
#include <core/system_command.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/memory_storage/memory_storage.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include "route.hpp"

namespace duck_charmer {

    using services::dispatcher::manager_dispatcher_t;

    constexpr static auto name_dispatcher = "dispatcher";

    base_spaces::base_spaces(const configuration::config& config)
        : resource(std::pmr::get_default_resource())
        , manager_dispatcher_(nullptr,actor_zeta::deleter(resource))
        , memory_storage_(nullptr,actor_zeta::deleter(resource))
        , manager_disk_()
        , manager_wal_()
        , wrapper_dispatcher_(nullptr,actor_zeta::deleter(resource))
        , scheduler_(new actor_zeta::shared_work(1, 1000))
        , scheduler_dispather_(new actor_zeta::shared_work(1, 1000)) {
        log_ = initialization_logger("python", config.log.path.c_str());
        log_.set_level(config.log.level);
        trace(log_, "spaces::spaces()");

        ///scheduler_.reset(new actor_zeta::shared_work(1, 1000), actor_zeta::detail::thread_pool_deleter());

        trace(log_, "manager_wal start");
        auto  manager_wal_address = actor_zeta::address_t::empty_address();
        if (config.wal.on) {
            auto wal  = actor_zeta::spawn_supervisor<services::wal::manager_wal_replicate_t>(resource, scheduler_.get(), config.wal, log_);
            manager_wal_address = wal->address();
            manager_wal_ = std::move(wal);
        } else {
            auto wal_empty = actor_zeta::spawn_supervisor<services::wal::manager_wal_replicate_empty_t>(resource, scheduler_.get(), log_);
            manager_wal_address = wal_empty->address();
            manager_wal_ = std::move(wal_empty);
        }
        trace(log_, "manager_wal finish");

        trace(log_, "manager_disk start");
        auto  manager_disk_address = actor_zeta::address_t::empty_address();
        if (config.disk.on) {
            auto disk = actor_zeta::spawn_supervisor<services::disk::manager_disk_t>(resource, scheduler_.get(), config.disk, log_);
            manager_disk_address = disk->address();
            manager_disk_ = std::move(disk);
        } else {
            auto disk_empty = actor_zeta::spawn_supervisor<services::disk::manager_disk_empty_t>(resource, scheduler_.get());
            manager_disk_address = disk_empty->address();
            manager_disk_ = std::move(disk_empty);
        }
        trace(log_, "manager_disk finish");

        trace(log_, "memory_storage start");
        memory_storage_ = actor_zeta::spawn_supervisor<services::memory_storage_t>(resource, scheduler_.get(), log_);
        trace(log_, "memory_storage finish");

        trace(log_, "manager_dispatcher start");
        manager_dispatcher_ = actor_zeta::spawn_supervisor<services::dispatcher::manager_dispatcher_t>(resource, scheduler_dispather_.get(), log_);
        trace(log_, "manager_dispatcher finish");

        wrapper_dispatcher_ =  actor_zeta::spawn_supervisor<wrapper_dispatcher_t>(resource, manager_dispatcher_->address(), log_);
        trace(log_, "manager_dispatcher create dispatcher");

        actor_zeta::send(
            manager_dispatcher_,
            actor_zeta::address_t::empty_address(),
            core::handler_id(core::route::sync),
            std::make_tuple(memory_storage_->address(), actor_zeta::address_t(manager_wal_address), actor_zeta::address_t(manager_disk_address)));

        actor_zeta::send(
            manager_wal_address,
            actor_zeta::address_t::empty_address(),
            core::handler_id(core::route::sync),
            std::make_tuple(actor_zeta::address_t( manager_disk_address), manager_dispatcher_->address()));

        actor_zeta::send(
            manager_disk_address,
            actor_zeta::address_t::empty_address(),
            core::handler_id(core::route::sync),
            std::make_tuple(manager_dispatcher_->address()));

        actor_zeta::send(
            memory_storage_,
            actor_zeta::address_t::empty_address(),
            core::handler_id(core::route::sync),
            std::make_tuple(manager_dispatcher_->address(), actor_zeta::address_t(manager_disk_address)));

        actor_zeta::send(manager_wal_address, actor_zeta::address_t::empty_address(), wal::handler_id(wal::route::create));
        actor_zeta::send(manager_disk_address, actor_zeta::address_t::empty_address(), disk::handler_id(disk::route::create_agent));
        manager_dispatcher_->create_dispatcher(name_dispatcher);
        scheduler_dispather_->start();
        scheduler_->start();
        trace(log_, "spaces::spaces() final");
    }

    log_t& base_spaces::get_log() {
        return log_;
    }

    wrapper_dispatcher_t* base_spaces::dispatcher() {
        return wrapper_dispatcher_.get();
    }

    base_spaces::~base_spaces() {
        trace(log_, "delete spaces");
        scheduler_->stop();
        scheduler_dispather_->stop();
    }

} // namespace python
