#include "dispatcher.hpp"

#include <core/system_command.hpp>
#include <core/tracy/tracy.hpp>

#include <components/document/document.hpp>
#include <components/planner/planner.hpp>
#include <components/ql/statements.hpp>

#include <services/collection/route.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/disk/route.hpp>
#include <services/memory_storage/route.hpp>
#include <services/wal/route.hpp>

using namespace components::ql;
using namespace components::cursor;

namespace services::dispatcher {

    dispatcher_t::dispatcher_t(manager_dispatcher_t* manager_dispatcher,
                               std::pmr::memory_resource* resource,
                               actor_zeta::address_t mstorage,
                               actor_zeta::address_t mwal,
                               actor_zeta::address_t mdisk,
                               log_t& log,
                               std::string name)
        : actor_zeta::basic_async_actor(manager_dispatcher, std::move(name))
        , log_(log.clone())
        , manager_dispatcher_(manager_dispatcher->address())
        , resource_(resource)
        , memory_storage_(std::move(mstorage))
        , manager_wal_(std::move(mwal))
        , manager_disk_(std::move(mdisk)) {
        trace(log_, "dispatcher_t::dispatcher_t start name:{}", type());
        add_handler(core::handler_id(core::route::load), &dispatcher_t::load);
        add_handler(disk::handler_id(disk::route::load_finish), &dispatcher_t::load_from_disk_result);
        add_handler(memory_storage::handler_id(memory_storage::route::load_finish),
                    &dispatcher_t::load_from_memory_resource_result);
        add_handler(wal::handler_id(wal::route::load_finish), &dispatcher_t::load_from_wal_result);
        add_handler(handler_id(route::execute_ql), &dispatcher_t::execute_ql);
        add_handler(memory_storage::handler_id(memory_storage::route::execute_plan_finish),
                    &dispatcher_t::execute_ql_finish);
        add_handler(collection::handler_id(collection::route::size), &dispatcher_t::size);
        add_handler(collection::handler_id(collection::route::size_finish), &dispatcher_t::size_finish);
        add_handler(collection::handler_id(collection::route::close_cursor), &dispatcher_t::close_cursor);
        add_handler(wal::handler_id(wal::route::success), &dispatcher_t::wal_success);
        trace(log_, "dispatcher_t::dispatcher_t finish name:{}", type());
    }

    dispatcher_t::~dispatcher_t() { trace(log_, "delete dispatcher_t"); }

    void dispatcher_t::load(components::session::session_id_t& session, actor_zeta::address_t sender) {
        trace(log_, "dispatcher_t::load, session: {}", session.data());
        load_session_ = session;
        make_session(session_to_address_, session, session_t(std::move(sender)));
        actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::load), session);
    }

    void dispatcher_t::load_from_disk_result(components::session::session_id_t& session,
                                             const disk::result_load_t& result) {
        trace(log_, "dispatcher_t::load_from_disk_result, session: {}, wal_id: {}", session.data(), result.wal_id());
        if ((*result).empty()) {
            actor_zeta::send(find_session(session_to_address_, session).address(),
                             dispatcher_t::address(),
                             core::handler_id(core::route::load_finish));
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

    void dispatcher_t::load_from_memory_resource_result(components::session::session_id_t& session) {
        trace(log_, "dispatcher_t::load_from_memory_resource_result, session: {}", session.data());
        actor_zeta::send(manager_disk_, address(), disk::handler_id(disk::route::load_indexes), session);
        actor_zeta::send(manager_wal_, address(), wal::handler_id(wal::route::load), session, load_result_.wal_id());
        load_result_.clear();
    }

    void dispatcher_t::load_from_wal_result(components::session::session_id_t& session,
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
                             core::handler_id(core::route::load_finish));
            remove_session(session_to_address_, session);
            return;
        }
        last_wal_id_ = records_[load_count_answers_ - 1].id;
        for (auto& record : records_) {
            switch (record.type) {
                case statement_type::create_database: {
                    assert(std::holds_alternative<create_database_t>(record.data) &&
                           "[dispatcher_t::load_from_wal_result]: [ case: statement_type::create_database] variant "
                           "record.data holds the alternative create_database_t");
                    auto& data = std::get<create_database_t>(record.data);
                    components::session::session_id_t session_database;
                    execute_ql(session_database, &data, manager_wal_);
                    break;
                }
                case statement_type::drop_database: {
                    assert(std::holds_alternative<drop_database_t>(record.data) &&
                           "[dispatcher_t::load_from_wal_result]: [ case: statement_type::drop_database] variant "
                           "record.data holds the alternative drop_database_t");
                    auto& data = std::get<drop_database_t>(record.data);
                    components::session::session_id_t session_database;
                    execute_ql(session_database, &data, manager_wal_);
                    break;
                }
                case statement_type::create_collection: {
                    assert(std::holds_alternative<create_collection_t>(record.data) &&
                           "[dispatcher_t::load_from_wal_result]: [ case: statement_type::create_collection] variant "
                           "record.data holds the alternative create_collection_t");
                    auto& data = std::get<create_collection_t>(record.data);
                    components::session::session_id_t session_collection;
                    execute_ql(session_collection, &data, manager_wal_);
                    break;
                }
                case statement_type::drop_collection: {
                    assert(std::holds_alternative<drop_collection_t>(record.data) &&
                           "[dispatcher_t::load_from_wal_result]: [ case: statement_type::drop_collection] variant "
                           "record.data holds the alternative drop_collection_t");
                    auto& data = std::get<drop_collection_t>(record.data);
                    components::session::session_id_t session_collection;
                    execute_ql(session_collection, &data, manager_wal_);
                    break;
                }
                case statement_type::insert_one: {
                    trace(log_, "dispatcher_t::load_from_wal_result: insert_one {}", session.data());
                    assert(std::holds_alternative<insert_one_t>(record.data) &&
                           "[dispatcher_t::load_from_wal_result]: [ case: statement_type::insert_one] variant "
                           "record.data holds the alternative insert_one_t");
                    auto& ql = std::get<insert_one_t>(record.data);
                    components::session::session_id_t session_insert;
                    execute_ql(session_insert, &ql, manager_wal_);
                    break;
                }
                case statement_type::insert_many: {
                    trace(log_, "dispatcher_t::load_from_wal_result: insert_many {}", session.data());
                    assert(std::holds_alternative<insert_many_t>(record.data) &&
                           "[dispatcher_t::load_from_wal_result]: [ case: statement_type::insert_many] variant "
                           "record.data holds the alternative insert_many_t");
                    auto& ql = std::get<insert_many_t>(record.data);
                    components::session::session_id_t session_insert;
                    execute_ql(session_insert, &ql, manager_wal_);
                    break;
                }
                case statement_type::delete_one: {
                    assert(std::holds_alternative<delete_one_t>(record.data) &&
                           "[dispatcher_t::load_from_wal_result]: [ case: statement_type::delete_one] variant "
                           "record.data holds the alternative delete_one_t");
                    auto& ql = std::get<delete_one_t>(record.data);
                    components::session::session_id_t session_delete;
                    execute_ql(session_delete, &ql, manager_wal_);
                    break;
                }
                case statement_type::delete_many: {
                    assert(std::holds_alternative<delete_many_t>(record.data) &&
                           "[dispatcher_t::load_from_wal_result]: [ case: statement_type::delete_many] variant "
                           "record.data holds the alternative delete_many_t");
                    auto& ql = std::get<delete_many_t>(record.data);
                    components::session::session_id_t session_delete;
                    execute_ql(session_delete, &ql, manager_wal_);
                    break;
                }
                case statement_type::update_one: {
                    trace(log_, "dispatcher_t::load_from_wal_result: update_one {}", session.data());
                    assert(std::holds_alternative<update_one_t>(record.data) &&
                           "[dispatcher_t::load_from_wal_result]: [ case: statement_type::update_one] variant "
                           "record.data holds the alternative update_one_t");
                    auto& ql = std::get<update_one_t>(record.data);
                    components::session::session_id_t session_update;
                    execute_ql(session_update, &ql, manager_wal_);
                    break;
                }
                case statement_type::update_many: {
                    assert(std::holds_alternative<update_many_t>(record.data) &&
                           "[dispatcher_t::load_from_wal_result]: [ case: statement_type::update_many] variant "
                           "record.data holds the alternative update_many_t");
                    auto& ql = std::get<update_many_t>(record.data);
                    components::session::session_id_t session_update;
                    execute_ql(session_update, &ql, manager_wal_);
                    break;
                }
                case statement_type::create_index: {
                    assert(std::holds_alternative<create_index_t>(record.data) &&
                           "[dispatcher_t::load_from_wal_result]: [ case: statement_type::create_index] variant "
                           "record.data holds the alternative create_index_t");
                    auto& ql = std::get<create_index_t>(record.data);
                    components::session::session_id_t session_create_index;
                    execute_ql(session_create_index, &ql, manager_wal_);
                    break;
                }
                // TODO no drop index?
                default:
                    break;
            }
        }
    }

    void dispatcher_t::execute_ql(components::session::session_id_t& session,
                                  ql_statement_t* ql,
                                  actor_zeta::base::address_t address) {
        trace(log_, "dispatcher_t::execute_ql: session {}, {}", session.data(), ql->to_string());
        make_session(session_to_address_, session, session_t(address, ql));
        auto logic_plan = create_logic_plan(ql);
        actor_zeta::send(memory_storage_,
                         dispatcher_t::address(),
                         memory_storage::handler_id(memory_storage::route::execute_plan),
                         session,
                         std::move(logic_plan.first),
                         std::move(logic_plan.second));
    }

    void dispatcher_t::execute_ql_finish(components::session::session_id_t& session, cursor_t_ptr result) {
        result_storage_[session] = result;
        auto& s = find_session(session_to_address_, session);
        auto* ql = s.get<ql_statement_t*>();
        trace(log_,
              "dispatcher_t::execute_ql_finish: session {}, {}, {}",
              session.data(),
              ql->to_string(),
              result->is_success());
        if (result->is_success()) {
            //todo: delete

            switch (ql->type()) {
                case statement_type::create_database: {
                    trace(log_, "dispatcher_t::execute_ql_finish: {}", to_string(ql->type()));
                    actor_zeta::send(manager_disk_,
                                     dispatcher_t::address(),
                                     disk::handler_id(disk::route::append_database),
                                     session,
                                     ql->database_);
                    if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::create_database),
                                         session,
                                         *static_cast<create_database_t*>(ql));
                        return;
                    }
                    break;
                }

                case statement_type::create_collection: {
                    trace(log_, "dispatcher_t::execute_ql_finish: {}", to_string(ql->type()));
                    actor_zeta::send(manager_disk_,
                                     dispatcher_t::address(),
                                     disk::handler_id(disk::route::append_collection),
                                     session,
                                     ql->database_,
                                     ql->collection_);
                    if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::create_collection),
                                         session,
                                         *static_cast<create_collection_t*>(ql));
                        return;
                    }
                    break;
                }

                case statement_type::insert_one: {
                    trace(log_, "dispatcher_t::execute_ql_finish: {}", to_string(ql->type()));
                    if (s.address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        assert(s.is_type<ql_statement_t*>() && "Doesn't holds ql_statement_t*");
                        auto statement = *static_cast<components::ql::insert_one_t*>(s.get<ql_statement_t*>());
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::insert_one),
                                         session,
                                         std::move(statement));
                        return;
                    }
                    break;
                }

                case statement_type::insert_many: {
                    trace(log_, "dispatcher_t::execute_ql_finish: {}", to_string(ql->type()));
                    if (s.address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        assert(s.is_type<ql_statement_t*>() && "Doesn't holds ql_statement_t*");
                        auto statement = *static_cast<components::ql::insert_many_t*>(s.get<ql_statement_t*>());
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::insert_many),
                                         session,
                                         std::move(statement));
                        return;
                    }
                    break;
                }
                case statement_type::update_one: {
                    trace(log_, "dispatcher_t::execute_ql_finish: {}", to_string(ql->type()));
                    if (s.address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        assert(s.is_type<ql_statement_t*>() && "Doesn't holds ql_statement_t*");
                        auto statement = *static_cast<components::ql::update_one_t*>(s.get<ql_statement_t*>());
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::update_one),
                                         session,
                                         std::move(statement));
                        return;
                    }
                    break;
                }

                case statement_type::update_many: {
                    trace(log_, "dispatcher_t::execute_ql_finish: {}", to_string(ql->type()));
                    if (s.address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        assert(s.is_type<ql_statement_t*>() && "Doesn't holds ql_statement_t*");
                        auto statement = *static_cast<components::ql::update_many_t*>(s.get<ql_statement_t*>());
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::update_many),
                                         session,
                                         std::move(statement));
                        return;
                    }
                    break;
                }

                case statement_type::delete_one: {
                    trace(log_, "dispatcher_t::execute_ql_finish: {}", to_string(ql->type()));
                    if (s.address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        assert(s.is_type<ql_statement_t*>() && "Doesn't holds ql_statement_t*");
                        auto statement = *static_cast<components::ql::delete_one_t*>(s.get<ql_statement_t*>());
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::delete_one),
                                         session,
                                         std::move(statement));
                        return;
                    }
                    break;
                }

                case statement_type::delete_many: {
                    trace(log_, "dispatcher_t::execute_ql_finish: {}", to_string(ql->type()));
                    if (s.address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        assert(s.is_type<ql_statement_t*>() && "Doesn't holds ql_statement_t*");
                        auto statement = *static_cast<components::ql::delete_many_t*>(s.get<ql_statement_t*>());
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::delete_many),
                                         session,
                                         std::move(statement));
                        return;
                    }
                    break;
                }

                case statement_type::drop_collection: {
                    trace(log_, "dispatcher_t::execute_ql_finish: {}", to_string(ql->type()));
                    collection_full_name_t name(ql->database_, ql->collection_);
                    actor_zeta::send(manager_disk_,
                                     dispatcher_t::address(),
                                     disk::handler_id(disk::route::remove_collection),
                                     session,
                                     ql->database_,
                                     ql->collection_);
                    if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::drop_collection),
                                         session,
                                         components::ql::drop_collection_t(ql->database_, ql->collection_));
                        return;
                    }
                    break;
                }
                case statement_type::create_index:
                case statement_type::drop_index: {
                    trace(log_, "dispatcher_t::execute_ql_finish: {}", to_string(ql->type()));
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
                    trace(log_, "dispatcher_t::execute_ql_finish: non processed type - {}", to_string(ql->type()));
                }
            }

            //end: delete
        }
        // TODO add verification for mutable types (they should be skipped too)
        if (!load_from_wal_in_progress(session)) {
            actor_zeta::send(s.address(),
                             dispatcher_t::address(),
                             handler_id(route::execute_ql_finish),
                             session,
                             std::move(result));
            remove_session(session_to_address_, session);
            result_storage_.erase(session);
        }
    }

    void dispatcher_t::size(components::session::session_id_t& session,
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

    void dispatcher_t::size_finish(components::session::session_id_t& session, cursor_t_ptr&& result) {
        trace(log_, "dispatcher_t::size_finish session: {}, size: {}", session.data(), result->size());
        actor_zeta::send(find_session(session_to_address_, session).address(),
                         dispatcher_t::address(),
                         collection::handler_id(collection::route::size_finish),
                         session,
                         result->size());
        remove_session(session_to_address_, session);
    }

    void dispatcher_t::close_cursor(components::session::session_id_t& session) {
        trace(log_, " dispatcher_t::close_cursor ");
        trace(log_, "Session : {}", session.data());
        auto it = cursor_.find(session);
        if (it != cursor_.end()) {
            std::set<collection_full_name_t> collections;
            for (auto& i : *it->second) {
                collections.insert(i->collection_name());
            }
            actor_zeta::send(memory_storage_,
                             address(),
                             collection::handler_id(collection::route::close_cursor),
                             session,
                             std::move(collections));
            cursor_.erase(it);
        } else {
            error(log_, "Not find session : {}", session.data());
        }
    }

    void dispatcher_t::wal_success(components::session::session_id_t& session, services::wal::id_t wal_id) {
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
                         handler_id(route::execute_ql_finish),
                         session,
                         result);
        remove_session(session_to_address_, session);
        result_storage_.erase(session);
    }

    // TODO separate change logic and condition check
    bool dispatcher_t::load_from_wal_in_progress(components::session::session_id_t& session) {
        if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
            if (--load_count_answers_ == 0) {
                actor_zeta::send(find_session(session_to_address_, load_session_).address(),
                                 dispatcher_t::address(),
                                 core::handler_id(core::route::load_finish));
                remove_session(session_to_address_, load_session_);
            }
            return true;
        }
        return false;
    }

    std::pair<components::logical_plan::node_ptr, components::ql::storage_parameters>
    dispatcher_t::create_logic_plan(ql_statement_t* statement) {
        //todo: cache logical plans
        components::planner::planner_t planner;
        auto logic_plan = planner.create_plan(resource_, statement);
        auto parameters = statement->is_parameters()
                              ? static_cast<components::ql::ql_param_statement_t*>(statement)->take_parameters()
                              : components::ql::storage_parameters{resource_};
        return {logic_plan, parameters};
    }

    manager_dispatcher_t::manager_dispatcher_t(std::pmr::memory_resource* mr,
                                               actor_zeta::scheduler_raw scheduler,
                                               log_t& log)
        : actor_zeta::cooperative_supervisor<manager_dispatcher_t>(mr, "manager_dispatcher")
        , log_(log.clone())
        , e_(scheduler) {
        ZoneScoped;
        trace(log_, "manager_dispatcher_t::manager_dispatcher_t ");
        add_handler(handler_id(route::create), &manager_dispatcher_t::create);
        add_handler(core::handler_id(core::route::load), &manager_dispatcher_t::load);
        add_handler(handler_id(route::execute_ql), &manager_dispatcher_t::execute_ql);
        add_handler(collection::handler_id(collection::route::size), &manager_dispatcher_t::size);
        add_handler(collection::handler_id(collection::route::close_cursor), &manager_dispatcher_t::close_cursor);
        add_handler(core::handler_id(core::route::sync), &manager_dispatcher_t::sync);
        trace(log_, "manager_dispatcher_t finish");
    }

    manager_dispatcher_t::~manager_dispatcher_t() {
        ZoneScoped;
        trace(log_, "delete manager_dispatcher_t");
    }

    auto manager_dispatcher_t::scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* { return e_; }

    //NOTE: behold thread-safety!
    auto manager_dispatcher_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        ZoneScoped;
        std::unique_lock<spin_lock> _(lock_);
        set_current_message(std::move(msg));
        execute(this, current_message());
    }

    void manager_dispatcher_t::create(components::session::session_id_t& session, std::string& name) {
        trace(log_, "manager_dispatcher_t::create session: {} , name: {} ", session.data(), name);
        auto target = spawn_actor<dispatcher_t>(
            [this, name](dispatcher_t* ptr) {
                dispatchers_.emplace_back(ptr, [&](dispatcher_t* agent) { mr_delete(resource(), agent); });
            },
            resource(),
            memory_storage_,
            manager_wal_,
            manager_disk_,
            log_,
            std::string(name));
    }

    void manager_dispatcher_t::load(components::session::session_id_t& session) {
        trace(log_, "manager_dispatcher_t::load session: {}", session.data());
        return actor_zeta::send(dispatcher(),
                                address(),
                                core::handler_id(core::route::load),
                                session,
                                current_message()->sender());
    }

    void manager_dispatcher_t::execute_ql(components::session::session_id_t& session, ql_statement_t* ql) {
        trace(log_, "manager_dispatcher_t::execute_ql session: {}, {}", session.data(), ql->to_string());
        return actor_zeta::send(dispatcher(),
                                address(),
                                handler_id(route::execute_ql),
                                session,
                                ql,
                                current_message()->sender());
    }

    void manager_dispatcher_t::size(components::session::session_id_t& session,
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

    void manager_dispatcher_t::close_cursor(components::session::session_id_t&) {}

    auto manager_dispatcher_t::dispatcher() -> actor_zeta::address_t { return dispatchers_[0]->address(); }

} // namespace services::dispatcher
