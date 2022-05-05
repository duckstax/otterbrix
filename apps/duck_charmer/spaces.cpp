#include "spaces.hpp"

#include <route.hpp>

#include <actor-zeta.hpp>

#include <memory>

namespace duck_charmer {
    spaces* spaces::instance_ = nullptr;

    spaces* spaces::get_instance() {
        if (instance_ == nullptr) {
            instance_ = new spaces();
        }
        return instance_;
    }

    wrapper_dispatcher_t* spaces::dispatcher() {
        return wrapper_dispatcher_.get();
    }

    using services::dispatcher::manager_dispatcher_t;
    constexpr static char* name_dispatcher = "dispatcher";
    spaces::spaces() {
        std::string log_dir("/tmp/");
        log_ = initialization_logger("duck_charmer", log_dir);
        log_.set_level(log_t::level::trace);
        trace(log_, "spaces::spaces()");
        boost::filesystem::path current_path = boost::filesystem::current_path();

        resource = actor_zeta::detail::pmr::get_default_resource();

        trace(log_, "manager_wal start");
        manager_wal_ = actor_zeta::spawn_supervisor<services::wal::manager_wal_replicate_t>(resource, current_path, log_, 1, 1000);
        actor_zeta::send(manager_wal_, actor_zeta::address_t::empty_address(), wal::route::create);
        trace(log_, "manager_wal finish");


        trace(log_, "manager_disk start");
        manager_disk_ = actor_zeta::spawn_supervisor<services::disk::manager_disk_t>(resource, current_path / "disk", log_, 1, 1000);
        actor_zeta::send(manager_disk_, actor_zeta::address_t::empty_address(), disk::route::create_agent);
        trace(log_, "manager_disk finish");

        trace(log_, "manager_database start");
        manager_database_ = actor_zeta::spawn_supervisor<services::database::manager_database_t>(resource, log_, 1, 1000);
        trace(log_, "manager_database finish");


        trace(log_, "manager_dispatcher start");
        manager_dispatcher_ = actor_zeta::spawn_supervisor<services::dispatcher::manager_dispatcher_t>(resource, log_, 1, 1000);
        manager_dispatcher_->create_dispatcher(name_dispatcher);
        trace(log_, "manager_dispatcher finish");

        wrapper_dispatcher_ = std::make_unique<wrapper_dispatcher_t>(resource, manager_dispatcher_->address(), log_);
        goblin_engineer::link(manager_wal_,manager_database_);
        goblin_engineer::link(manager_wal_,manager_dispatcher_);
        goblin_engineer::link(manager_database_, manager_dispatcher_);
        goblin_engineer::link(manager_disk_, manager_dispatcher_);
        trace(log_, "manager_dispatcher create dispatcher");
        auto tmp = wrapper_dispatcher_->address();
        auto tmp_1 = manager_dispatcher_->address();
        goblin_engineer::link(tmp_1, tmp);

        trace(log_, "spaces::spaces() final");
    }

    log_t& spaces::get_log() {
        return log_;
    }

} // namespace duck_charmer