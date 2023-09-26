#include "manager_wal_replicate.hpp"

#include "route.hpp"

#include <core/system_command.hpp>

namespace services::wal {

    base_manager_wal_replicate_t::base_manager_wal_replicate_t(actor_zeta::detail::pmr::memory_resource* mr, actor_zeta::scheduler_raw scheduler)
        : actor_zeta::cooperative_supervisor<base_manager_wal_replicate_t>(mr, "manager_wal")
        , e_(scheduler) {
    }

    auto base_manager_wal_replicate_t::scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* {
        return e_;
    }

    auto base_manager_wal_replicate_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        std::unique_lock<spin_lock> _(lock_);
        set_current_message(std::move(msg));
        execute(this,current_message());
    }

    manager_wal_replicate_t::manager_wal_replicate_t(actor_zeta::detail::pmr::memory_resource* mr, actor_zeta::scheduler_raw scheduler,
                                                     configuration::config_wal config, log_t& log)
        : base_manager_wal_replicate_t(mr, scheduler)
        , config_(std::move(config))
        , log_(log.clone())
        , create_(resource(),handler_id(route::create),this, &manager_wal_replicate_t::create_wal_worker))
        , load_(resource(),handler_id(route::load),this, &manager_wal_replicate_t::load))
        , create_database_(resource(),handler_id(route::create_database),this, &manager_wal_replicate_t::create_database))
        , drop_database_(resource(),handler_id(route::drop_database),this, &manager_wal_replicate_t::drop_database))
        , create_collection_(resource(),handler_id(route::create_collection),this, &manager_wal_replicate_t::create_collection))
        , drop_collection_(resource(),handler_id(route::drop_collection),this, &manager_wal_replicate_t::drop_collection))
        , insert_one_(resource(),handler_id(route::insert_one),this, &manager_wal_replicate_t::insert_one))
        , insert_many_(resource(),handler_id(route::insert_many),this, &manager_wal_replicate_t::insert_many))
        , delete_one_(resource(),handler_id(route::delete_one),this, &manager_wal_replicate_t::delete_one))
        , delete_many_(resource(),handler_id(route::delete_many),this, &manager_wal_replicate_t::delete_many))
        , update_one_(resource(),handler_id(route::update_one),this, &manager_wal_replicate_t::update_one))
        , update_many_(resource(),handler_id(route::update_many),this, &manager_wal_replicate_t::update_many))
        , core_sync_(resource(),core::handler_id(core::route::sync),this, &manager_wal_replicate_t::sync))
        , create_index_(resource(),handler_id(route::create_index),this, &manager_wal_replicate_t::create_index)) {
        trace(log_, "manager_wal_replicate_t");
    }

    manager_wal_replicate_t::~manager_wal_replicate_t() {
        trace(log_, "delete manager_wal_replicate_t");
    }

    void manager_wal_replicate_t::create_wal_worker() {
        if (config_.sync_to_disk) {
            trace(log_, "manager_wal_replicate_t::create_wal_worker");
            auto address = spawn_actor<wal_replicate_t>([this](wal_replicate_t* ptr) {
                dispatchers_.emplace_back(wal_replicate_ptr(ptr));
            }, log_, config_);
        } else {
            trace(log_, "manager_wal_replicate_t::create_wal_worker without disk");
            auto address = spawn_actor<wal_replicate_without_disk_t>([this](wal_replicate_t* ptr) {
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

    manager_wal_replicate_empty_t::manager_wal_replicate_empty_t(actor_zeta::detail::pmr::memory_resource* mr, actor_zeta::scheduler_raw scheduler, log_t& log)
        : base_manager_wal_replicate_t(mr, scheduler)
    , create_(resource(),handler_id(route::create),this, &manager_wal_replicate_empty_t::nothing<>))
    , load_(resource(),handler_id(route::load),this, &manager_wal_replicate_empty_t::nothing<session_id_t&, services::wal::id_t>))
    , create_database_(resource(),handler_id(route::create_database),this, &manager_wal_replicate_empty_t::nothing<session_id_t&, ql::create_database_t&>))
    , drop_database_(resource(),handler_id(route::drop_database),this, &manager_wal_replicate_empty_t::nothing<session_id_t&, ql::drop_database_t&>))
    , create_collection_(resource(),handler_id(route::create_collection),this, &manager_wal_replicate_empty_t::nothing<session_id_t&, ql::create_collection_t&>))
    , drop_collection_(resource(),handler_id(route::drop_collection),this, &manager_wal_replicate_empty_t::nothing<session_id_t&, ql::drop_collection_t&>))
    , insert_one_(resource(),handler_id(route::insert_one),this, &manager_wal_replicate_empty_t::nothing<session_id_t&, ql::insert_one_t&>))
    , insert_many_(resource(),handler_id(route::insert_many),this, &manager_wal_replicate_empty_t::nothing<session_id_t&, ql::insert_many_t&>))
    , delete_one_(resource(),handler_id(route::delete_one),this, &manager_wal_replicate_empty_t::nothing<session_id_t&, ql::delete_one_t&>))
    , delete_many_(resource(),handler_id(route::delete_many),this, &manager_wal_replicate_empty_t::nothing<session_id_t&, ql::delete_many_t&>))
    , update_one_(resource(),handler_id(route::update_one),this, &manager_wal_replicate_empty_t::nothing<session_id_t&, ql::update_one_t&>))
    , update_many_(resource(),handler_id(route::update_many),this, &manager_wal_replicate_empty_t::nothing<session_id_t&, ql::update_many_t&>))
    , core_sync_(resource(),core::handler_id(core::route::sync),this, &manager_wal_replicate_empty_t::nothing<address_pack&>))
    , create_index_(resource(),handler_id(route::create_index),this, &manager_wal_replicate_empty_t::nothing<session_id_t&, ql::create_index_t&>))
    {
        trace(log, "manager_wal_replicate_empty_t");
    }

} //namespace services::wal
