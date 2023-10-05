#include "manager_wal_replicate.hpp"

#include "route.hpp"

#include <core/system_command.hpp>

namespace services::wal {

    base_manager_wal_replicate_t::base_manager_wal_replicate_t(std::pmr::memory_resource* mr, actor_zeta::scheduler_raw scheduler)
        : actor_zeta::cooperative_supervisor<base_manager_wal_replicate_t>(mr)
        , e_(scheduler) {
    }

    auto base_manager_wal_replicate_t::make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t* {
        return e_;
    }

    auto base_manager_wal_replicate_t::make_type() const noexcept -> const char* const {
        return "manager_wal";
    }

    manager_wal_replicate_t::manager_wal_replicate_t(std::pmr::memory_resource* mr, actor_zeta::scheduler_raw scheduler,
                                                     configuration::config_wal config, log_t& log)
        : base_manager_wal_replicate_t(mr, scheduler)
        , config_(std::move(config))
        , log_(log.clone())
        , create_(actor_zeta::make_behavior(resource(),handler_id(route::create),this, &manager_wal_replicate_t::create_wal_worker))
        , load_(actor_zeta::make_behavior(resource(),handler_id(route::load),this, &manager_wal_replicate_t::load))
        , create_database_(actor_zeta::make_behavior(resource(),handler_id(route::create_database),this, &manager_wal_replicate_t::create_database))
        , drop_database_(actor_zeta::make_behavior(resource(),handler_id(route::drop_database),this, &manager_wal_replicate_t::drop_database))
        , create_collection_(actor_zeta::make_behavior(resource(),handler_id(route::create_collection),this, &manager_wal_replicate_t::create_collection))
        , drop_collection_(actor_zeta::make_behavior(resource(),handler_id(route::drop_collection),this, &manager_wal_replicate_t::drop_collection))
        , insert_one_(actor_zeta::make_behavior(resource(),handler_id(route::insert_one),this, &manager_wal_replicate_t::insert_one))
        , insert_many_(actor_zeta::make_behavior(resource(),handler_id(route::insert_many),this, &manager_wal_replicate_t::insert_many))
        , delete_one_(actor_zeta::make_behavior(resource(),handler_id(route::delete_one),this, &manager_wal_replicate_t::delete_one))
        , delete_many_(actor_zeta::make_behavior(resource(),handler_id(route::delete_many),this, &manager_wal_replicate_t::delete_many))
        , update_one_(actor_zeta::make_behavior(resource(),handler_id(route::update_one),this, &manager_wal_replicate_t::update_one))
        , update_many_(actor_zeta::make_behavior(resource(),handler_id(route::update_many),this, &manager_wal_replicate_t::update_many))
        , core_sync_(actor_zeta::make_behavior(resource(),core::handler_id(core::route::sync),this, &manager_wal_replicate_t::sync))
        , create_index_(actor_zeta::make_behavior(resource(),handler_id(route::create_index),this, &manager_wal_replicate_t::create_index)) {
        trace(log_, "manager_wal_replicate_t");
    }

    manager_wal_replicate_t::~manager_wal_replicate_t() {
        trace(log_, "delete manager_wal_replicate_t");
    }

    void manager_wal_replicate_t::create_wal_worker() {
        if (config_.sync_to_disk) {
            trace(log_, "manager_wal_replicate_t::create_wal_worker");
            auto address = spawn_actor([this](wal_replicate_t* ptr) {
                dispatchers_.emplace_back(wal_replicate_ptr(ptr));
            }, log_, config_);
        } else {
            trace(log_, "manager_wal_replicate_t::create_wal_worker without disk");
            auto address = spawn_actor([this](wal_replicate_t* ptr) {
                dispatchers_.emplace_back(wal_replicate_ptr(ptr));
            }, log_, config_);
        }
    }

    void manager_wal_replicate_t::load(session_id_t& session, services::wal::id_t wal_id) {
        trace(log_, "manager_wal_replicate_t::load, id: {}", wal_id);
        actor_zeta::send(dispatchers_[0]->address(), address(), handler_id(route::load), session, current_message()->sender(), wal_id);
    }

    void manager_wal_replicate_t::create_database(session_id_t& session, components::ql::create_database_t& data) {
        trace(log_, "manager_wal_replicate_t::create_database {}", data.database_);
        actor_zeta::send(dispatchers_[0]->address(), address(), handler_id(route::create_database), session, current_message()->sender(), std::move(data));
    }

    void manager_wal_replicate_t::drop_database(session_id_t& session, components::ql::drop_database_t& data) {
        trace(log_, "manager_wal_replicate_t::drop_database {}", data.database_);
        actor_zeta::send(dispatchers_[0]->address(), address(), handler_id(route::drop_database), session, current_message()->sender(), std::move(data));
    }

    void manager_wal_replicate_t::create_collection(session_id_t& session, components::ql::create_collection_t& data) {
        trace(log_, "manager_wal_replicate_t::create_collection {}::{}", data.database_, data.collection_);
        actor_zeta::send(dispatchers_[0]->address(), address(), handler_id(route::create_collection), session, current_message()->sender(), std::move(data));
    }

    void manager_wal_replicate_t::drop_collection(session_id_t& session, components::ql::drop_collection_t& data) {
        trace(log_, "manager_wal_replicate_t::drop_collection {}::{}", data.database_, data.collection_);
        actor_zeta::send(dispatchers_[0]->address(), address(), handler_id(route::drop_collection), session, current_message()->sender(), std::move(data));
    }

    void manager_wal_replicate_t::insert_one(session_id_t& session, components::ql::insert_one_t& data) {
        trace(log_, "manager_wal_replicate_t::insert_one");
        actor_zeta::send(dispatchers_[0]->address(), address(), handler_id(route::insert_one), session, current_message()->sender(), std::move(data));
    }

    void manager_wal_replicate_t::insert_many(session_id_t& session, components::ql::insert_many_t& data) {
        trace(log_, "manager_wal_replicate_t::insert_many");
        actor_zeta::send(dispatchers_[0]->address(), address(), handler_id(route::insert_many), session, current_message()->sender(), std::move(data));
    }

    void manager_wal_replicate_t::delete_one(session_id_t& session, components::ql::delete_one_t& data) {
        trace(log_, "manager_wal_replicate_t::delete_one");
        actor_zeta::send(dispatchers_[0]->address(), address(), handler_id(route::delete_one), session, current_message()->sender(), std::move(data));
    }

    void manager_wal_replicate_t::delete_many(session_id_t& session, components::ql::delete_many_t& data) {
        trace(log_, "manager_wal_replicate_t::delete_many");
        actor_zeta::send(dispatchers_[0]->address(), address(), handler_id(route::delete_many), session, current_message()->sender(), std::move(data));
    }

    void manager_wal_replicate_t::update_one(session_id_t& session, components::ql::update_one_t& data) {
        trace(log_, "manager_wal_replicate_t::update_one");
        actor_zeta::send(dispatchers_[0]->address(), address(), handler_id(route::update_one), session, current_message()->sender(), std::move(data));
    }

    void manager_wal_replicate_t::update_many(session_id_t& session, components::ql::update_many_t& data) {
        trace(log_, "manager_wal_replicate_t::update_many");
        actor_zeta::send(dispatchers_[0]->address(), address(), handler_id(route::update_many), session, current_message()->sender(), std::move(data));
    }

    void manager_wal_replicate_t::create_index(session_id_t& session, components::ql::create_index_t& data) {
        trace(log_, "manager_wal_replicate_t::create_index");
        actor_zeta::send(dispatchers_[0]->address(), address(), handler_id(route::create_index), session, current_message()->sender(), std::move(data));
    }

    auto manager_wal_replicate_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        std::unique_lock<spin_lock> _(lock_);
        set_current_message(std::move(msg));
        behavior()(current_message());
    }

    actor_zeta::behavior_t manager_wal_replicate_t::behavior() {
        return actor_zeta::make_behavior(
            resource(),
            [this](actor_zeta::message* msg) -> void {
                switch (msg->command()) {
                    case handler_id(route::create): {
                        create_(msg);
                        break;
                    }
                    case handler_id(route::load): {
                        load_(msg);
                        break;
                    }
                    case handler_id(route::create_database): {
                        create_database_(msg);
                        break;
                    }
                    case handler_id(route::drop_database): {
                        drop_database_(msg);
                        break;
                    }
                    case handler_id(route::create_collection): {
                        create_collection_(msg);
                        break;
                    }
                    case handler_id(route::drop_collection): {
                        drop_collection_(msg);
                        break;
                    }
                    case handler_id(route::insert_one): {
                        insert_one_(msg);
                        break;
                    }
                    case handler_id(route::insert_many): {
                        insert_many_(msg);
                        break;
                    }
                    case handler_id(route::delete_one): {
                        delete_one_(msg);
                        break;
                    }
                    case handler_id(route::delete_many): {
                        delete_many_(msg);
                        break;
                    }
                    case handler_id(route::update_one): {
                        update_one_(msg);
                        break;
                    }
                    case handler_id(route::update_many): {
                        update_many_(msg);
                        break;
                    }
                    case core::handler_id(core::route::sync): {
                        core_sync_(msg);
                        break;
                    }
                    case handler_id(route::create_index): {
                        create_index_(msg);
                        break;
                    }
                }
            });
    }

    manager_wal_replicate_empty_t::manager_wal_replicate_empty_t(std::pmr::memory_resource* mr, actor_zeta::scheduler_raw scheduler, log_t& log)
        : base_manager_wal_replicate_t(mr, scheduler){
        trace(log, "manager_wal_replicate_empty_t");
    }

    actor_zeta::behavior_t manager_wal_replicate_empty_t::behavior() {
        return actor_zeta::make_behavior(
            resource(),
            [this](actor_zeta::message* msg) -> void {
                switch (msg->command()) {
                    case handler_id(route::create):
                    case handler_id(route::load):
                    case handler_id(route::create_database):
                    case handler_id(route::drop_database):
                    case handler_id(route::create_collection):
                    case handler_id(route::drop_collection):
                    case handler_id(route::insert_one):
                    case handler_id(route::insert_many):
                    case handler_id(route::delete_one):
                    case handler_id(route::delete_many):
                    case handler_id(route::update_one):
                    case handler_id(route::update_many):
                    case core::handler_id(core::route::sync):
                    case handler_id(route::create_index): {
                        break;
                    }
                }
            });
    }

} //namespace services::wal
