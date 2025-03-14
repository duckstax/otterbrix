#include "dispatcher.hpp"

#include <core/system_command.hpp>
#include <core/tracy/tracy.hpp>

#include <components/document/document.hpp>
#include <components/planner/planner.hpp>

#include <services/collection/route.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/disk/route.hpp>
#include <services/memory_storage/route.hpp>
#include <services/wal/route.hpp>

using namespace components::logical_plan;
using namespace components::cursor;

namespace services::dispatcher {

    dispatcher_t::dispatcher_t(manager_dispatcher_t* manager_dispatcher,
                               actor_zeta::address_t& mstorage,
                               actor_zeta::address_t& mwal,
                               actor_zeta::address_t& mdisk,
                               log_t& log)
        : actor_zeta::basic_actor<dispatcher_t>(manager_dispatcher)
        , load_(actor_zeta::make_behavior(resource(), core::handler_id(core::route::load), this, &dispatcher_t::load))
        , load_from_disk_result_(actor_zeta::make_behavior(resource(),
                                                           disk::handler_id(disk::route::load_finish),
                                                           this,
                                                           &dispatcher_t::load_from_disk_result))
        , load_from_memory_storage_result_(
              actor_zeta::make_behavior(resource(),
                                        memory_storage::handler_id(memory_storage::route::load_finish),
                                        this,
                                        &dispatcher_t::load_from_memory_storage_result))
        , load_from_wal_result_(actor_zeta::make_behavior(resource(),
                                                          wal::handler_id(wal::route::load_finish),
                                                          this,
                                                          &dispatcher_t::load_from_wal_result))
        , execute_plan_(
              actor_zeta::make_behavior(resource(), handler_id(route::execute_plan), this, &dispatcher_t::execute_plan))
        , execute_plan_finish_(
              actor_zeta::make_behavior(resource(),
                                        memory_storage::handler_id(memory_storage::route::execute_plan_finish),
                                        this,
                                        &dispatcher_t::execute_plan_finish))
        , size_(actor_zeta::make_behavior(resource(),
                                          collection::handler_id(collection::route::size),
                                          this,
                                          &dispatcher_t::size))
        , size_finish_(actor_zeta::make_behavior(resource(),
                                                 collection::handler_id(collection::route::size_finish),
                                                 this,
                                                 &dispatcher_t::size_finish))
        , close_cursor_(actor_zeta::make_behavior(resource(),
                                                  collection::handler_id(collection::route::close_cursor),
                                                  this,
                                                  &dispatcher_t::close_cursor))
        , wal_success_(actor_zeta::make_behavior(resource(),
                                                 wal::handler_id(wal::route::success),
                                                 this,
                                                 &dispatcher_t::wal_success))
        , log_(log.clone())
        , manager_dispatcher_(manager_dispatcher->address())
        , memory_storage_(mstorage)
        , manager_wal_(mwal)
        , manager_disk_(mdisk) {
        trace(log_, "dispatcher_t::dispatcher_t start name:{}", make_type());
    }

    dispatcher_t::~dispatcher_t() { trace(log_, "delete dispatcher_t"); }

    auto dispatcher_t::make_type() const noexcept -> const char* const { return "dispatcher_t"; }

    actor_zeta::behavior_t dispatcher_t::behavior() {
        return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
            switch (msg->command()) {
                case core::handler_id(core::route::load): {
                    load_(msg);
                    break;
                }

                case disk::handler_id(disk::route::load_finish): {
                    load_from_disk_result_(msg);
                    break;
                }
                case memory_storage::handler_id(memory_storage::route::load_finish): {
                    load_from_memory_storage_result_(msg);
                    break;
                }
                case wal::handler_id(wal::route::load_finish): {
                    load_from_wal_result_(msg);
                    break;
                }
                case handler_id(route::execute_plan): {
                    execute_plan_(msg);
                    break;
                }
                case memory_storage::handler_id(memory_storage::route::execute_plan_finish): {
                    execute_plan_finish_(msg);
                    break;
                }
                case collection::handler_id(collection::route::size): {
                    size_(msg);
                    break;
                }
                case collection::handler_id(collection::route::size_finish): {
                    size_finish_(msg);
                    break;
                }
                case collection::handler_id(collection::route::close_cursor): {
                    close_cursor_(msg);
                    break;
                }
                case wal::handler_id(wal::route::success): {
                    wal_success_(msg);
                    break;
                }
            }
        });
    }

    void dispatcher_t::load(const components::session::session_id_t& session, actor_zeta::address_t sender) {
        trace(log_, "dispatcher_t::load, session: {}", session.data());
        load_session_ = session;
        make_session(session_to_address_, session, session_t(std::move(sender)));
        actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::load), session);
    }

    void dispatcher_t::load_from_disk_result(const components::session::session_id_t& session,
                                             const disk::result_load_t& result) {
        trace(log_, "dispatcher_t::load_from_disk_result, session: {}, wal_id: {}", session.data(), result.wal_id());
        if ((*result).empty()) {
            actor_zeta::send(find_session(session_to_address_, session).address(),
                             dispatcher_t::address(),
                             core::handler_id(core::route::load_finish),
                             session);
            remove_session(session_to_address_, session);
            load_result_.clear();
        } else {
            load_result_ = result;
            actor_zeta::send(memory_storage_,
                             address(),
                             memory_storage::handler_id(memory_storage::route::load),
                             session,
                             result);
        }
    }

    void dispatcher_t::load_from_memory_storage_result(const components::session::session_id_t& session) {
        trace(log_, "dispatcher_t::load_from_memory_storage_result, session: {}", session.data());
        actor_zeta::send(manager_disk_, address(), disk::handler_id(disk::route::load_indexes), session);
        actor_zeta::send(manager_wal_, address(), wal::handler_id(wal::route::load), session, load_result_.wal_id());
        load_result_.clear();
    }

    void dispatcher_t::load_from_wal_result(const components::session::session_id_t& session,
                                            std::vector<services::wal::record_t>& in_records) {
        // TODO think what to do with records
        records_ = std::move(in_records);
        load_count_answers_ = records_.size();
        trace(log_,
              "dispatcher_t::load_from_wal_result, session: {}, count commands: {}",
              session.data(),
              load_count_answers_);
        if (load_count_answers_ == 0) {
            trace(log_, "dispatcher_t::load_from_wal_result - empty records_");
            actor_zeta::send(find_session(session_to_address_, session).address(),
                             dispatcher_t::address(),
                             core::handler_id(core::route::load_finish),
                             session);
            remove_session(session_to_address_, session);
            return;
        }
        last_wal_id_ = records_[load_count_answers_ - 1].id;
        for (auto& record : records_) {
            switch (record.type) {
                case node_type::create_database_t: {
                    assert(record.data->type() == record.type &&
                           "[dispatcher_t::load_from_wal_result]: [ case: node_type::create_database] variant "
                           "record.data holds the alternative create_database_t");
                    components::session::session_id_t session_database;
                    execute_plan(session_database, record.data, record.params, manager_wal_);
                    break;
                }
                case node_type::drop_database_t: {
                    assert(record.data->type() == record.type &&
                           "[dispatcher_t::load_from_wal_result]: [ case: node_type::drop_database] variant "
                           "record.data holds the alternative drop_database_t");
                    components::session::session_id_t session_database;
                    execute_plan(session_database, record.data, record.params, manager_wal_);
                    break;
                }
                case node_type::create_collection_t: {
                    assert(record.data->type() == record.type &&
                           "[dispatcher_t::load_from_wal_result]: [ case: node_type::create_collection] variant "
                           "record.data holds the alternative create_collection_t");
                    components::session::session_id_t session_collection;
                    execute_plan(session_collection, record.data, record.params, manager_wal_);
                    break;
                }
                case node_type::drop_collection_t: {
                    assert(record.data->type() == record.type &&
                           "[dispatcher_t::load_from_wal_result]: [ case: node_type::drop_collection] variant "
                           "record.data holds the alternative drop_collection_t");
                    components::session::session_id_t session_collection;
                    execute_plan(session_collection, record.data, record.params, manager_wal_);
                    break;
                }
                case node_type::insert_t: {
                    trace(log_, "dispatcher_t::load_from_wal_result: insert_one {}", session.data());
                    assert(record.data->type() == record.type &&
                           "[dispatcher_t::load_from_wal_result]: [ case: node_type::insert_t] variant "
                           "record.data holds the alternative insert_one_t");
                    components::session::session_id_t session_insert;
                    execute_plan(session_insert, record.data, record.params, manager_wal_);
                    break;
                }
                case node_type::delete_t: {
                    assert(record.data->type() == record.type &&
                           "[dispatcher_t::load_from_wal_result]: [ case: node_type::delete_t] variant "
                           "record.data holds the alternative delete_one_t");
                    components::session::session_id_t session_delete;
                    execute_plan(session_delete, record.data, record.params, manager_wal_);
                    break;
                }
                case node_type::update_t: {
                    trace(log_, "dispatcher_t::load_from_wal_result: update_one {}", session.data());
                    assert(record.data->type() == record.type &&
                           "[dispatcher_t::load_from_wal_result]: [ case: node_type::update_t] variant "
                           "record.data holds the alternative update_one_t");
                    components::session::session_id_t session_update;
                    execute_plan(session_update, record.data, record.params, manager_wal_);
                    break;
                }
                case node_type::create_index_t: {
                    assert(record.data->type() == record.type &&
                           "[dispatcher_t::load_from_wal_result]: [ case: node_type::create_index] variant "
                           "record.data holds the alternative create_index_t");
                    components::session::session_id_t session_create_index;
                    execute_plan(session_create_index, record.data, record.params, manager_wal_);
                    break;
                }
                // TODO no drop index?
                default:
                    break;
            }
        }
    }

    void dispatcher_t::execute_plan(const components::session::session_id_t& session,
                                    components::logical_plan::node_ptr plan,
                                    parameter_node_ptr params,
                                    actor_zeta::base::address_t address) {
        trace(log_, "dispatcher_t::execute_plan: session {}, {}", session.data(), plan->to_string());
        make_session(session_to_address_, session, session_t(address, plan, params));
        auto logic_plan = create_logic_plan(plan);
        actor_zeta::send(memory_storage_,
                         dispatcher_t::address(),
                         memory_storage::handler_id(memory_storage::route::execute_plan),
                         session,
                         std::move(logic_plan),
                         params->take_parameters());
    }

    void dispatcher_t::execute_plan_finish(const components::session::session_id_t& session, cursor_t_ptr result) {
        result_storage_[session] = result;
        auto& s = find_session(session_to_address_, session);
        auto plan = s.node();
        auto params = s.params();
        trace(log_,
              "dispatcher_t::execute_plan_finish: session {}, {}, {}",
              session.data(),
              plan->to_string(),
              result->is_success());
        if (result->is_success()) {
            //todo: delete

            switch (plan->type()) {
                case node_type::create_database_t: {
                    trace(log_, "dispatcher_t::execute_plan_finish: {}", to_string(plan->type()));
                    actor_zeta::send(manager_disk_,
                                     dispatcher_t::address(),
                                     disk::handler_id(disk::route::append_database),
                                     session,
                                     plan->database_name());
                    if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::create_database),
                                         session,
                                         plan);
                        return;
                    }
                    break;
                }

                case node_type::create_collection_t: {
                    trace(log_, "dispatcher_t::execute_plan_finish: {}", to_string(plan->type()));
                    actor_zeta::send(manager_disk_,
                                     dispatcher_t::address(),
                                     disk::handler_id(disk::route::append_collection),
                                     session,
                                     plan->database_name(),
                                     plan->collection_name());
                    if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::create_collection),
                                         session,
                                         plan);
                        return;
                    }
                    break;
                }

                case node_type::insert_t: {
                    trace(log_, "dispatcher_t::execute_plan_finish: {}", to_string(plan->type()));
                    if (s.address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        assert(s.node() && "Doesn't holds logical_plan");
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::insert_many),
                                         session,
                                         std::move(plan));
                        return;
                    }
                    break;
                }

                case node_type::update_t: {
                    trace(log_, "dispatcher_t::execute_plan_finish: {}", to_string(plan->type()));
                    if (s.address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        assert(s.node() && "Doesn't holds correct plan*");
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::update_many),
                                         session,
                                         std::move(plan),
                                         s.params());
                        return;
                    }
                    break;
                }

                case node_type::delete_t: {
                    trace(log_, "dispatcher_t::execute_plan_finish: {}", to_string(plan->type()));
                    if (s.address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        assert(s.node() && "Doesn't holds correct plan*");
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::delete_many),
                                         session,
                                         std::move(plan),
                                         s.params());
                        return;
                    }
                    break;
                }

                case node_type::drop_collection_t: {
                    trace(log_, "dispatcher_t::execute_plan_finish: {}", to_string(plan->type()));
                    collection_full_name_t name(plan->database_name(), plan->collection_name());
                    actor_zeta::send(manager_disk_,
                                     dispatcher_t::address(),
                                     disk::handler_id(disk::route::remove_collection),
                                     session,
                                     plan->database_name(),
                                     plan->collection_name());
                    if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::drop_collection),
                                         session,
                                         std::move(plan));
                        return;
                    }
                    break;
                }
                case node_type::create_index_t:
                case node_type::drop_index_t: {
                    trace(log_, "dispatcher_t::execute_plan_finish: {}", to_string(plan->type()));
                    const auto session_address = find_session(session_to_address_, session).address().get();
                    if (session_address == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    }
                    // We initiate session from this dispatcher,
                    // for now it means: dispatcher was called from manager_disk after initiate loading from disk.
                    if (session_address == dispatcher_t::address().get()) {
                        remove_session(session_to_address_, session);
                        return;
                    }
                    break;
                }
                default: {
                    trace(log_, "dispatcher_t::execute_plan_finish: non processed type - {}", to_string(plan->type()));
                }
            }

            //end: delete
        }
        // TODO add verification for mutable types (they should be skipped too)
        if (!load_from_wal_in_progress(session)) {
            actor_zeta::send(s.address(),
                             dispatcher_t::address(),
                             handler_id(route::execute_plan_finish),
                             session,
                             std::move(result));
            remove_session(session_to_address_, session);
            result_storage_.erase(session);
        }
    }

    void dispatcher_t::size(const components::session::session_id_t& session,
                            std::string& database_name,
                            std::string& collection,
                            actor_zeta::base::address_t sender) {
        trace(log_,
              "dispatcher_t::size: session:{}, database: {}, collection: {}",
              session.data(),
              database_name,
              collection);
        make_session(session_to_address_, session, session_t(std::move(sender)));
        actor_zeta::send(memory_storage_,
                         dispatcher_t::address(),
                         collection::handler_id(collection::route::size),
                         session,
                         collection_full_name_t{database_name, collection});
    }

    void dispatcher_t::size_finish(const components::session::session_id_t& session, cursor_t_ptr&& result) {
        trace(log_, "dispatcher_t::size_finish session: {}, size: {}", session.data(), result->size());
        actor_zeta::send(find_session(session_to_address_, session).address(),
                         dispatcher_t::address(),
                         collection::handler_id(collection::route::size_finish),
                         session,
                         result->size());
        remove_session(session_to_address_, session);
    }

    void dispatcher_t::close_cursor(const components::session::session_id_t& session) {
        trace(log_, " dispatcher_t::close_cursor ");
        trace(log_, "Session : {}", session.data());
        auto it = cursor_.find(session);
        if (it != cursor_.end()) {
            std::set<collection_full_name_t> collections;
            for (auto& i : *it->second) {
                collections.insert(i->collection_name());
            }
            cursor_.erase(it);
        } else {
            error(log_, "Not find session : {}", session.data());
        }
    }

    void dispatcher_t::wal_success(const components::session::session_id_t& session, services::wal::id_t wal_id) {
        trace(log_, "dispatcher_t::wal_success session : {}, wal id: {}", session.data(), wal_id);
        actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::flush), session, wal_id);

        if (!is_session_exist(session_to_address_, session)) {
            return;
        }

        auto session_obj = find_session(session_to_address_, session);
        const bool is_from_wal = session_obj.address().get() == manager_wal_.get();
        if (is_from_wal) {
            return;
        }

        trace(log_, "dispatcher_t::wal_success remove session : {}, wal id: {}", session.data(), wal_id);
        auto result = result_storage_[session];
        actor_zeta::send(session_obj.address(),
                         dispatcher_t::address(),
                         handler_id(route::execute_plan_finish),
                         session,
                         result);
        remove_session(session_to_address_, session);
        result_storage_.erase(session);
    }

    // TODO separate change logic and condition check
    bool dispatcher_t::load_from_wal_in_progress(const components::session::session_id_t& session) {
        if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
            if (--load_count_answers_ == 0) {
                actor_zeta::send(find_session(session_to_address_, load_session_).address(),
                                 dispatcher_t::address(),
                                 core::handler_id(core::route::load_finish),
                                 load_session_);
                remove_session(session_to_address_, load_session_);
            }
            return true;
        }
        return false;
    }

    components::logical_plan::node_ptr dispatcher_t::create_logic_plan(components::logical_plan::node_ptr plan) {
        //todo: cache logical plans
        components::planner::planner_t planner;
        return planner.create_plan(resource(), std::move(plan));
    }

    manager_dispatcher_t::manager_dispatcher_t(std::pmr::memory_resource* resource_ptr,
                                               actor_zeta::scheduler_raw scheduler,
                                               log_t& log)
        : actor_zeta::cooperative_supervisor<manager_dispatcher_t>(resource_ptr)
        , create_(actor_zeta::make_behavior(resource(), handler_id(route::create), this, &manager_dispatcher_t::create))
        , load_(actor_zeta::make_behavior(resource(),
                                          core::handler_id(core::route::load),
                                          this,
                                          &manager_dispatcher_t::load))
        , execute_plan_(actor_zeta::make_behavior(resource(),
                                                  handler_id(route::execute_plan),
                                                  this,
                                                  &manager_dispatcher_t::execute_plan))
        , size_(actor_zeta::make_behavior(resource(),
                                          collection::handler_id(collection::route::size),
                                          this,
                                          &manager_dispatcher_t::size))
        , close_cursor_(actor_zeta::make_behavior(resource(),
                                                  collection::handler_id(collection::route::close_cursor),
                                                  this,
                                                  &manager_dispatcher_t::close_cursor))
        , sync_(actor_zeta::make_behavior(resource(),
                                          core::handler_id(core::route::sync),
                                          this,
                                          &manager_dispatcher_t::sync))
        , log_(log.clone())
        , e_(scheduler) {
        ZoneScoped;
        trace(log_, "manager_dispatcher_t::manager_dispatcher_t ");
    }

    manager_dispatcher_t::~manager_dispatcher_t() {
        ZoneScoped;
        trace(log_, "delete manager_dispatcher_t");
    }

    auto manager_dispatcher_t::make_type() const noexcept -> const char* const { return "manager_dispatcher"; }

    auto manager_dispatcher_t::make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t* { return e_; }

    actor_zeta::behavior_t manager_dispatcher_t::behavior() {
        return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
            switch (msg->command()) {
                case handler_id(route::create): {
                    create_(msg);
                    break;
                }
                case core::handler_id(core::route::load): {
                    load_(msg);
                    break;
                }
                case handler_id(route::execute_plan): {
                    execute_plan_(msg);
                    break;
                }
                case collection::handler_id(collection::route::size): {
                    size_(msg);
                    break;
                }
                case collection::handler_id(collection::route::close_cursor): {
                    close_cursor_(msg);
                    break;
                }
                case core::handler_id(core::route::sync): {
                    sync_(msg);
                    break;
                }
            }
        });
    }

    //NOTE: behold thread-safety!
    auto manager_dispatcher_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        ZoneScoped;
        std::unique_lock<spin_lock> _(lock_);
        set_current_message(std::move(msg));
        behavior()(current_message());
    }

    // mr_delete(resource(), agent)
    void manager_dispatcher_t::create(const components::session::session_id_t& session) {
        trace(log_, "manager_dispatcher_t::create session: {} ", session.data());
        auto target = spawn_actor(
            [this](dispatcher_t* ptr) {
                dispatchers_.emplace_back(dispatcher_ptr(ptr, actor_zeta::pmr::deleter_t(resource())));
            },
            memory_storage_,
            manager_wal_,
            manager_disk_,
            log_);
    }

    void manager_dispatcher_t::load(const components::session::session_id_t& session) {
        trace(log_, "manager_dispatcher_t::load session: {}", session.data());
        return actor_zeta::send(dispatcher(),
                                address(),
                                core::handler_id(core::route::load),
                                session,
                                current_message()->sender());
    }

    void manager_dispatcher_t::execute_plan(const components::session::session_id_t& session,
                                            node_ptr plan,
                                            parameter_node_ptr params) {
        trace(log_, "manager_dispatcher_t::execute_plan session: {}, {}", session.data(), plan->to_string());
        return actor_zeta::send(dispatcher(),
                                address(),
                                handler_id(route::execute_plan),
                                session,
                                std::move(plan),
                                std::move(params),
                                current_message()->sender());
    }

    void manager_dispatcher_t::size(const components::session::session_id_t& session,
                                    std::string& database_name,
                                    std::string& collection) {
        trace(log_,
              "manager_dispatcher_t::size session: {} , database: {}, collection name: {} ",
              session.data(),
              database_name,
              collection);
        actor_zeta::send(dispatcher(),
                         address(),
                         collection::handler_id(collection::route::size),
                         session,
                         std::move(database_name),
                         std::move(collection),
                         current_message()->sender());
    }

    void manager_dispatcher_t::close_cursor(const components::session::session_id_t&) {}

    auto manager_dispatcher_t::dispatcher() -> actor_zeta::address_t { return dispatchers_[0]->address(); }

} // namespace services::dispatcher
