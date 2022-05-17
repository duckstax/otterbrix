#include "spaces.hpp"

#include <route.hpp>

#include <actor-zeta.hpp>

#include <memory>

#include "core/system_command.hpp"

namespace duck_charmer {
    spaces* spaces::instance_ = nullptr;

    spaces* spaces::get_instance() {
        if (instance_ == nullptr) {
            instance_ = new spaces(components::config::default_config());
        }
        return instance_;
    }

    void spaces::reload(const components::config &config) {
        if (instance_ != nullptr) {
            throw std::runtime_error("spaces already initialized");
        }
        instance_ = new spaces(config);
    }

    wrapper_dispatcher_t* spaces::dispatcher() {
        return wrapper_dispatcher_.get();
    }

    using services::dispatcher::manager_dispatcher_t;

    constexpr static char* name_dispatcher = "dispatcher";

    spaces::spaces(const components::config& config)
        : scheduler_(new actor_zeta::shared_work(1, 1000), actor_zeta::detail::thread_pool_deleter()) {
        log_ = initialization_logger("duck_charmer", config.log.path.c_str());
        log_.set_level(config.log.level);
        trace(log_, "spaces::spaces()");

        ///scheduler_.reset(new actor_zeta::shared_work(1, 1000), actor_zeta::detail::thread_pool_deleter());
        resource = actor_zeta::detail::pmr::get_default_resource();

        trace(log_, "manager_wal start");
        manager_wal_ = actor_zeta::spawn_supervisor<services::wal::manager_wal_replicate_t>(resource, scheduler_.get(), config.wal.path, log_);
        trace(log_, "manager_wal finish");


        trace(log_, "manager_disk start");
        manager_disk_ = actor_zeta::spawn_supervisor<services::disk::manager_disk_t>(resource, scheduler_.get(), config.disk.path, log_);
        trace(log_, "manager_disk finish");

        trace(log_, "manager_database start");
        manager_database_ = actor_zeta::spawn_supervisor<services::database::manager_database_t>(resource, scheduler_.get(), log_);
        trace(log_, "manager_database finish");


        trace(log_, "manager_dispatcher start");
        manager_dispatcher_ = actor_zeta::spawn_supervisor<services::dispatcher::manager_dispatcher_t>(resource, scheduler_.get(), log_);
        trace(log_, "manager_dispatcher finish");

        wrapper_dispatcher_ = std::make_unique<wrapper_dispatcher_t>(resource, manager_dispatcher_->address(), log_);
        trace(log_, "manager_dispatcher create dispatcher");

        actor_zeta::send(
            manager_dispatcher_,
            actor_zeta::address_t::empty_address(),
            core::handler_id(core::route::sync),
            std::make_tuple(manager_database_->address(), manager_wal_->address(), manager_disk_->address()));

        actor_zeta::send(
            manager_wal_,
            actor_zeta::address_t::empty_address(),
            core::handler_id(core::route::sync),
            std::make_tuple(manager_disk_->address(), manager_dispatcher_->address()));


        actor_zeta::send(
            manager_disk_,
            actor_zeta::address_t::empty_address(),
            core::handler_id(core::route::sync),
            std::make_tuple(manager_dispatcher_->address()));

        actor_zeta::send(
            manager_database_,
            actor_zeta::address_t::empty_address(),
            core::handler_id(core::route::sync),
            std::make_tuple(manager_dispatcher_->address()));

        actor_zeta::send(manager_wal_, actor_zeta::address_t::empty_address(), wal::handler_id(wal::route::create));
        actor_zeta::send(manager_disk_, actor_zeta::address_t::empty_address(), disk::handler_id(disk::route::create_agent));
        manager_dispatcher_->create_dispatcher(name_dispatcher);
        scheduler_->start();
        trace(log_, "spaces::spaces() final");
    }

    log_t& spaces::get_log() {
        return log_;
    }

} // namespace duck_charmer