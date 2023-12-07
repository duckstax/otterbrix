#include "dispatcher.hpp"

#include <core/tracy/tracy.hpp>
#include <core/system_command.hpp>

#include <components/document/document.hpp>
#include <components/ql/statements.hpp>
#include <components/translator/ql_translator.hpp>

#include <services/collection/route.hpp>
#include <services/memory_storage/route.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/disk/route.hpp>
#include <services/wal/route.hpp>

using namespace components::ql;
using namespace components::cursor;

namespace services::dispatcher {

    dispatcher_t::dispatcher_t(
        manager_dispatcher_t* manager_dispatcher,
        std::pmr::memory_resource *resource,
        actor_zeta::address_t mstorage,
        actor_zeta::address_t mwal,
        actor_zeta::address_t mdisk,
        log_t& log,
        std::string name)
        : actor_zeta::basic_async_actor(manager_dispatcher, std::move(name))
        , log_(log.clone())
        , resource_(resource)
        , manager_dispatcher_(manager_dispatcher->address())
        , memory_storage_(std::move(mstorage))
        , manager_wal_(std::move(mwal))
        , manager_disk_(std::move(mdisk)) {
        trace(log_, "dispatcher_t::dispatcher_t start name:{}", type());
        add_handler(core::handler_id(core::route::load), &dispatcher_t::load);
        add_handler(disk::handler_id(disk::route::load_finish), &dispatcher_t::load_from_disk_result);
        add_handler(memory_storage::handler_id(memory_storage::route::load_finish), &dispatcher_t::load_from_memory_resource_result);
        add_handler(wal::handler_id(wal::route::load_finish), &dispatcher_t::load_from_wal_result);
        add_handler(handler_id(route::execute_ql), &dispatcher_t::execute_ql);
        add_handler(memory_storage::handler_id(memory_storage::route::execute_plan_finish), &dispatcher_t::execute_ql_finish);
        add_handler(collection::handler_id(collection::route::delete_documents), &dispatcher_t::delete_documents);
        add_handler(collection::handler_id(collection::route::delete_finish), &dispatcher_t::delete_finish);
        add_handler(collection::handler_id(collection::route::update_documents), &dispatcher_t::update_documents);
        add_handler(collection::handler_id(collection::route::update_finish), &dispatcher_t::update_finish);
        add_handler(collection::handler_id(collection::route::size), &dispatcher_t::size);
        add_handler(collection::handler_id(collection::route::size_finish), &dispatcher_t::size_finish);
        add_handler(collection::handler_id(collection::route::close_cursor), &dispatcher_t::close_cursor);
        add_handler(collection::handler_id(collection::route::create_index), &dispatcher_t::create_index);
        add_handler(collection::handler_id(collection::route::create_index_finish), &dispatcher_t::create_index_finish);
        add_handler(collection::handler_id(collection::route::drop_index), &dispatcher_t::drop_index);
        add_handler(collection::handler_id(collection::route::drop_index_finish), &dispatcher_t::drop_index_finish);
        add_handler(wal::handler_id(wal::route::success), &dispatcher_t::wal_success);
        trace(log_, "dispatcher_t::dispatcher_t finish name:{}", type());
    }

    dispatcher_t::~dispatcher_t() {
        trace(log_, "delete dispatcher_t");
    }

    void dispatcher_t::load(components::session::session_id_t &session, actor_zeta::address_t sender) {
        trace(log_, "dispatcher_t::load, session: {}", session.data());
        load_session_ = session;
        make_session(session_to_address_, session, session_t(std::move(sender)));
        actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::load), session);
    }

    void dispatcher_t::load_from_disk_result(components::session::session_id_t &session, const disk::result_load_t &result) {
        trace(log_, "dispatcher_t::load_from_disk_result, session: {}, wal_id: {}", session.data(), result.wal_id());
        if ((*result).empty()) {
            actor_zeta::send(find_session(session_to_address_, session).address(), dispatcher_t::address(), core::handler_id(core::route::load_finish));
            remove_session(session_to_address_, session);
            load_result_.clear();
        } else {
            load_result_ = result;
            actor_zeta::send(memory_storage_, address(), memory_storage::handler_id(memory_storage::route::load), session, result);
        }
    }

    void dispatcher_t::load_from_memory_resource_result(components::session::session_id_t &session, list_addresses_t collections) {
        trace(log_, "dispatcher_t::load_from_memory_resource_result, session: {}", session.data());
        for (const auto& collection : collections.addresses) {
            collection_address_book_.emplace(collection.name, collection.address);
        }
        actor_zeta::send(manager_disk_, address(), disk::handler_id(disk::route::load_indexes), session);
        actor_zeta::send(manager_wal_, address(), wal::handler_id(wal::route::load), session, load_result_.wal_id());
        load_result_.clear();
    }

    void dispatcher_t::load_from_wal_result(components::session::session_id_t& session, std::vector<services::wal::record_t>& in_records) {
        // TODO think what to do with records
        records_ = std::move(in_records);
        load_count_answers_ = records_.size();
        trace(log_, "dispatcher_t::load_from_wal_result, session: {}, count commands: {}", session.data(), load_count_answers_);
        if (load_count_answers_ == 0) {
            trace(log_, "dispatcher_t::load_from_wal_result - empty records_");
            actor_zeta::send(find_session(session_to_address_, session).address(), dispatcher_t::address(), core::handler_id(core::route::load_finish));
            remove_session(session_to_address_, session);
            return;
        }
        last_wal_id_ = records_[load_count_answers_ - 1].id;
        for (auto &record : records_) {
            switch (record.type) {
                case statement_type::create_database: {
                    assert(std::holds_alternative<create_database_t>(record.data) && "[dispatcher_t::load_from_wal_result]: [ case: statement_type::create_database] variant record.data holds the alternative create_database_t");
                    auto& data = std::get<create_database_t>(record.data);
                    components::session::session_id_t session_database;
                    execute_ql(session_database, &data, manager_wal_);
                    break;
                }
                    case statement_type::drop_database: {
                    assert(std::holds_alternative<drop_database_t>(record.data) && "[dispatcher_t::load_from_wal_result]: [ case: statement_type::drop_database] variant record.data holds the alternative drop_database_t");
                    auto& data = std::get<drop_database_t>(record.data);
                    components::session::session_id_t session_database;
                    execute_ql(session_database, &data, manager_wal_);
                    break;
                }
                    case statement_type::create_collection: {
                    assert(std::holds_alternative<create_collection_t>(record.data) && "[dispatcher_t::load_from_wal_result]: [ case: statement_type::create_collection] variant record.data holds the alternative create_collection_t");
                    auto& data = std::get<create_collection_t>(record.data);
                    components::session::session_id_t session_collection;
                    execute_ql(session_collection, &data, manager_wal_);
                    break;
                }
                case statement_type::drop_collection: {
                    assert(std::holds_alternative<drop_collection_t>(record.data) && "[dispatcher_t::load_from_wal_result]: [ case: statement_type::drop_collection] variant record.data holds the alternative drop_collection_t");
                    auto& data = std::get<drop_collection_t>(record.data);
                    components::session::session_id_t session_collection;
                    execute_ql(session_collection, &data, manager_wal_);
                    break;
                }
                case statement_type::insert_one: {
                    trace(log_, "dispatcher_t::load_from_wal_result: insert_one {}", session.data());
                    assert(std::holds_alternative<insert_one_t>(record.data) && "[dispatcher_t::load_from_wal_result]: [ case: statement_type::insert_one] variant record.data holds the alternative insert_one_t");
                    auto& ql = std::get<insert_one_t>(record.data);
                    components::session::session_id_t session_insert;
                    execute_ql(session_insert, &ql, manager_wal_);
                    break;
                }
                case statement_type::insert_many: {
                    trace(log_, "dispatcher_t::load_from_wal_result: insert_many {}", session.data());
                    assert(std::holds_alternative<insert_many_t>(record.data) && "[dispatcher_t::load_from_wal_result]: [ case: statement_type::insert_many] variant record.data holds the alternative insert_many_t");
                    auto& ql = std::get<insert_many_t>(record.data);
                    components::session::session_id_t session_insert;
                    execute_ql(session_insert, &ql, manager_wal_);
                    break;
                }
                case statement_type::delete_one: {
                    assert(std::holds_alternative<delete_one_t>(record.data) && "[dispatcher_t::load_from_wal_result]: [ case: statement_type::delete_one] variant record.data holds the alternative delete_one_t");
                    auto& ql = std::get<delete_one_t>(record.data);
                    components::session::session_id_t session_delete;
                    delete_documents(session_delete, &ql, manager_wal_);
                    break;
                }
                case statement_type::delete_many: {
                    assert(std::holds_alternative<delete_many_t>(record.data) && "[dispatcher_t::load_from_wal_result]: [ case: statement_type::delete_many] variant record.data holds the alternative delete_many_t");
                    auto& ql = std::get<delete_many_t>(record.data);
                    components::session::session_id_t session_delete;
                    delete_documents(session_delete, &ql, manager_wal_);
                    break;
                }
                case statement_type::update_one: {
                    trace(log_, "dispatcher_t::load_from_wal_result: update_one {}", session.data());
                    assert(std::holds_alternative<update_one_t>(record.data) && "[dispatcher_t::load_from_wal_result]: [ case: statement_type::update_one] variant record.data holds the alternative update_one_t");
                    auto& ql = std::get<update_one_t>(record.data);
                    components::session::session_id_t session_update;
                    update_documents(session_update, &ql, manager_wal_);
                    break;
                }
                case statement_type::update_many: {
                    assert(std::holds_alternative<update_many_t>(record.data) && "[dispatcher_t::load_from_wal_result]: [ case: statement_type::update_many] variant record.data holds the alternative update_many_t");
                    auto& ql = std::get<update_many_t>(record.data);
                    components::session::session_id_t session_update;
                    update_documents(session_update, &ql, manager_wal_);
                    break;
                }
                case statement_type::create_index: {
                    assert(std::holds_alternative<create_index_t>(record.data) && "[dispatcher_t::load_from_wal_result]: [ case: statement_type::create_index] variant record.data holds the alternative create_index_t");
                    auto& data = std::get<create_index_t>(record.data);
                    components::session::session_id_t session_create_index;
                    create_index(session_create_index, data, manager_wal_);
                    break;
                }
                default:
                    break;
            }
        }
    }

    void dispatcher_t::execute_ql(components::session::session_id_t& session, ql_statement_t* ql, actor_zeta::base::address_t address) {
        trace(log_, "dispatcher_t::execute_ql: session {}, {}", session.data(), ql->to_string());
        make_session(session_to_address_, session, session_t(address, ql));
        auto logic_plan = create_logic_plan(ql);
        actor_zeta::send(memory_storage_, dispatcher_t::address(), memory_storage::handler_id(memory_storage::route::execute_plan), session,
                         std::move(logic_plan.first), std::move(logic_plan.second));
    }

    void dispatcher_t::execute_ql_finish(components::session::session_id_t& session, cursor_t_ptr result) {
        result_storage_[session] = result;
        auto& s = find_session(session_to_address_, session);
        auto* ql = s.get<ql_statement_t*>();
        trace(log_, "dispatcher_t::execute_ql_finish: session {}, {}, {}", session.data(), ql->to_string(), result->is_success());
        if (result->is_success()) {
            //todo: delete
            
            if (ql->type() == statement_type::create_database) {
                trace(log_, "dispatcher_t::execute_ql_finish: create_database");
                actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::append_database), session, ql->database_);
                if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
                    wal_success(session, last_wal_id_);
                } else {
                    actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::create_database), session, *static_cast<create_database_t*>(ql));
                    return;
                }
            }

            if (ql->type() == statement_type::create_collection) {
                trace(log_, "dispatcher_t::execute_ql_finish: create_collection");
                collection_address_book_.emplace(collection_full_name_t(ql->database_, ql->collection_), result->get_address(0));
                actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::append_collection), session, ql->database_, ql->collection_);
                if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
                    wal_success(session, last_wal_id_);
                } else {
                    actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::create_collection), session, *static_cast<create_collection_t*>(ql));
                    return;
                }
            }
            

            if(ql->type() == statement_type::insert_one){
                trace(log_, "dispatcher_t::execute_ql_finish: insert_one");
                if (s.address().get() == manager_wal_.get()) {
                    wal_success(session, last_wal_id_);
                } else {
                    assert(s.is_type<ql_statement_t*>() && "Doesn't holds ql_statement_t*");
                    auto result = *static_cast<components::ql::insert_one_t*>(s.get<ql_statement_t*>());
                    actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::insert_one), session, std::move(result));
                    return;
                }
            }

            if(ql->type() == statement_type::insert_many){
                trace(log_, "dispatcher_t::execute_ql_finish: insert_many");
                if (s.address().get() == manager_wal_.get()) {
                    wal_success(session, last_wal_id_);
                } else {
                    assert(s.is_type<ql_statement_t*>() && "Doesn't holds ql_statement_t*");
                    auto result = *static_cast<components::ql::insert_many_t*>(s.get<ql_statement_t*>());
                    actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::insert_many), session, std::move(result));
                    return;
                }
            }

            if (ql->type() == statement_type::drop_collection) {
                trace(log_, "dispatcher_t::execute_ql_finish: drop_collection");
                collection_full_name_t name(ql->database_, ql->collection_);
                collection_address_book_.erase(name);
                actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::remove_collection), session, ql->database_, ql->collection_);
                if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
                    wal_success(session, last_wal_id_);
                } else {
                    actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::drop_collection), session, components::ql::drop_collection_t(ql->database_, ql->collection_));
                    return;
                }
            }

            //end: delete
        }
        // TODO add verification for mutable types (they should be skipped too)
        if (!check_load_from_wal(session)) {
            remove_session(session_to_address_, session);
            result_storage_.erase(session);
        }
    }

    void dispatcher_t::delete_documents(components::session::session_id_t& session, components::ql::ql_statement_t* statement, actor_zeta::address_t address) {
        debug(log_, "dispatcher_t::delete_one: session:{}, database: {}, collection: {}", session.data(), statement->database_, statement->collection_);
        collection_full_name_t key(statement->database_, statement->collection_);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            if (statement->type() == statement_type::delete_one) {
                make_session(session_to_address_, session, session_t(address, *static_cast<delete_one_t*>(statement)));
            } else {
                make_session(session_to_address_, session, session_t(address, *static_cast<delete_many_t*>(statement)));
            }
            auto logic_plan = create_logic_plan(statement);
            actor_zeta::send(it_collection->second, dispatcher_t::address(), collection::handler_id(collection::route::delete_documents), session, std::move(logic_plan.first), std::move(logic_plan.second));
        } else {
            actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::delete_finish), session, make_cursor(resource_));
        }
    }

    void dispatcher_t::delete_finish(components::session::session_id_t& session, cursor_t_ptr result) {
        trace(log_, "dispatcher_t::delete_finish session: {}", session.data());
        auto& s = find_session(session_to_address_, session);
        if (s.address().get() == manager_wal_.get()) {
            wal_success(session, last_wal_id_);
        } else {
            if (s.type() == statement_type::delete_one) {
                actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::delete_one), session, s.get<delete_one_t>());
            } else {
                actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::delete_many), session, s.get<delete_many_t>());
            }
        }
        if (!check_load_from_wal(session)) {
            actor_zeta::send(s.address(), dispatcher_t::address(), collection::handler_id(collection::route::delete_finish), session, result);
            remove_session(session_to_address_, session);
        }
    }

    void dispatcher_t::update_documents(components::session::session_id_t& session, components::ql::ql_statement_t* statement, actor_zeta::address_t address) {
        debug(log_, "dispatcher_t::update_one: session:{}, database: {}, collection: {}", session.data(), statement->database_, statement->collection_);
        collection_full_name_t key(statement->database_, statement->collection_);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            if (statement->type() == statement_type::update_one) {
                make_session(session_to_address_, session, session_t(address, *static_cast<update_one_t*>(statement)));
            } else {
                make_session(session_to_address_, session, session_t(address, *static_cast<update_many_t*>(statement)));
            }
            auto logic_plan = create_logic_plan(statement);
            actor_zeta::send(it_collection->second, dispatcher_t::address(), collection::handler_id(collection::route::update_documents), session, std::move(logic_plan.first), std::move(logic_plan.second));
        } else {
            actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::update_finish), session, make_cursor(resource_));
        }
    }

    void dispatcher_t::update_finish(components::session::session_id_t& session, cursor_t_ptr result) {
        trace(log_, "dispatcher_t::update_finish session: {}", session.data());
        auto& s = find_session(session_to_address_, session);
        if (s.address().get() == manager_wal_.get()) {
            wal_success(session, last_wal_id_);
        } else {
            if (s.type() == statement_type::update_one) {
                actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::update_one), session, s.get<update_one_t>());
            } else {
                actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::update_many), session, s.get<update_many_t>());
            }
        }
        if (!check_load_from_wal(session)) {
            actor_zeta::send(s.address(), dispatcher_t::address(), collection::handler_id(collection::route::update_finish), session, result);
            remove_session(session_to_address_, session);
        }
    }

    void dispatcher_t::size(components::session::session_id_t& session, std::string& database_name, std::string& collection, actor_zeta::address_t address) {
        trace(log_, "dispatcher_t::size: session:{}, database: {}, collection: {}", session.data(), database_name, collection);
        collection_full_name_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            make_session(session_to_address_, session, session_t(std::move(address)));
            actor_zeta::send(it_collection->second, dispatcher_t::address(), collection::handler_id(collection::route::size), session);
        } else {
            actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::size_finish), session,
                             make_cursor(actor_zeta::detail::pmr::get_default_resource(), operation_status_t::success));
        }
    }

    void dispatcher_t::size_finish(components::session::session_id_t& session, cursor_t_ptr result) {
        trace(log_, "dispatcher_t::size_finish session: {}", session.data());
        actor_zeta::send(find_session(session_to_address_, session).address(), dispatcher_t::address(), collection::handler_id(collection::route::size_finish), session, result);
        remove_session(session_to_address_, session);
    }

    void dispatcher_t::create_index(components::session::session_id_t &session, components::ql::create_index_t index, actor_zeta::address_t address) {
        debug(log_, "dispatcher_t::create_index: session:{}, index: {}", session.data(), index.name());
        collection_full_name_t key(index.database_, index.collection_);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            make_session(session_to_address_, session_key_t{session, index.name()}, session_t(address, index));
            actor_zeta::send(it_collection->second, dispatcher_t::address(), collection::handler_id(collection::route::create_index), session, std::move(index));
        } else {
            if (address.get() != dispatcher_t::address().get()) {
                actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::create_index_finish), session,
                                 make_cursor(actor_zeta::detail::pmr::get_default_resource(), operation_status_t::success));
            }
        }
    }

    void dispatcher_t::create_index_finish(components::session::session_id_t &session, const std::string& name, cursor_t_ptr result) {
        trace(log_, "dispatcher_t::create_index_finish session: {}, index: {}", session.data(), name);
        if (find_session(session_to_address_, session, name).address().get() == manager_wal_.get()) {
            wal_success(session, last_wal_id_);
        }
        if (find_session(session_to_address_, session, name).address().get() != dispatcher_t::address().get()) {
            actor_zeta::send(find_session(session_to_address_, session, name).address(), dispatcher_t::address(), collection::handler_id(collection::route::create_index_finish), session, result);
        }
        remove_session(session_to_address_, session, name);
    }

    void dispatcher_t::drop_index(components::session::session_id_t &session, components::ql::drop_index_t drop_index, actor_zeta::address_t address) {
        debug(log_, "dispatcher_t::drop_index: session: {}, index: {}", session.data(), drop_index.name());
        collection_full_name_t key(drop_index.database_, drop_index.collection_);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            make_session(session_to_address_, session_key_t{session, drop_index.name()}, session_t(address, drop_index));
            actor_zeta::send(it_collection->second, dispatcher_t::address(), collection::handler_id(collection::route::drop_index), session, std::move(drop_index));
        } else {
            if (address.get() != dispatcher_t::address().get()) {
                actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::drop_index_finish), session,
                                 make_cursor(actor_zeta::detail::pmr::get_default_resource(), operation_status_t::success));
            }
        }
    }

    void dispatcher_t::drop_index_finish(components::session::session_id_t &session, const std::string& name, cursor_t_ptr result) {
        trace(log_, "dispatcher_t::drop_index_finish session: {}, index: {}", session.data(), name);
        if (find_session(session_to_address_, session, name).address().get() == manager_wal_.get()) {
            wal_success(session, last_wal_id_);
        }
        if (find_session(session_to_address_, session, name).address().get() != dispatcher_t::address().get()) {
            actor_zeta::send(find_session(session_to_address_, session, name).address(), dispatcher_t::address(), collection::handler_id(collection::route::drop_index_finish), session, result);
        }
        remove_session(session_to_address_, session, name);
    }

    void dispatcher_t::close_cursor(components::session::session_id_t& session) {
        trace(log_, " dispatcher_t::close_cursor ");
        trace(log_, "Session : {}", session.data());
        auto it = cursor_.find(session);
        if (it != cursor_.end()) {
            for (auto& i : *it->second) {
                actor_zeta::send(i->address(), address(), collection::handler_id(collection::route::close_cursor), session);
            }
            cursor_.erase(it);
        } else {
            error(log_, "Not find session : {}", session.data());
        }
    }

    void dispatcher_t::wal_success(components::session::session_id_t& session, services::wal::id_t wal_id) {
        trace(log_, "dispatcher_t::wal_success session : {}, wal id: {}", session.data(), wal_id);
        actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::flush), session, wal_id);

        if(!is_session_exist(session_to_address_, session)){
            return;
        }
        
        auto session_obj = find_session(session_to_address_, session);
        const bool is_from_wal = session_obj.address().get() == manager_wal_.get();
        if(is_from_wal){
            return;
        }

        trace(log_, "dispatcher_t::wal_success remove session : {}, wal id: {}", session.data(), wal_id);
        auto result = result_storage_[session];
        actor_zeta::send(session_obj.address(), dispatcher_t::address(), handler_id(route::execute_ql_finish), session, result);
        remove_session(session_to_address_, session);
        result_storage_.erase(session);
        

    }

    bool dispatcher_t::check_load_from_wal(components::session::session_id_t& session) {
        if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
            if (--load_count_answers_ == 0) {
                actor_zeta::send(find_session(session_to_address_, load_session_).address(), dispatcher_t::address(), core::handler_id(core::route::load_finish));
                remove_session(session_to_address_, load_session_);
            }
            return true;
        }
        return false;
    }

    std::pair<components::logical_plan::node_ptr, components::ql::storage_parameters> dispatcher_t::create_logic_plan(
            ql_statement_t* statement) {
        //todo: cache logical plans
        auto logic_plan = components::translator::ql_translator(resource_, statement);
        auto parameters = statement->is_parameters()
                ? static_cast<components::ql::ql_param_statement_t*>(statement)->take_parameters()
                : components::ql::storage_parameters{};
        return {logic_plan, parameters};
    }


    manager_dispatcher_t::manager_dispatcher_t(
        actor_zeta::detail::pmr::memory_resource* mr,
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
        add_handler(collection::handler_id(collection::route::delete_documents), &manager_dispatcher_t::delete_documents);
        add_handler(collection::handler_id(collection::route::update_documents), &manager_dispatcher_t::update_documents);
        add_handler(collection::handler_id(collection::route::size), &manager_dispatcher_t::size);
        add_handler(collection::handler_id(collection::route::close_cursor), &manager_dispatcher_t::close_cursor);
        add_handler(collection::handler_id(collection::route::create_index), &manager_dispatcher_t::create_index);
        add_handler(collection::handler_id(collection::route::drop_index), &manager_dispatcher_t::drop_index);
        add_handler(core::handler_id(core::route::sync), &manager_dispatcher_t::sync);
        trace(log_, "manager_dispatcher_t finish");
    }

    manager_dispatcher_t::~manager_dispatcher_t() {
        ZoneScoped;
        trace(log_, "delete manager_dispatcher_t");
    }

    auto manager_dispatcher_t::scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* {
        return e_;
    }

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
                dispatchers_.emplace_back(dispatcher_ptr(ptr));
            },
            resource(), memory_storage_, manager_wal_, manager_disk_, log_, std::string(name));
    }

    void manager_dispatcher_t::load(components::session::session_id_t &session) {
        trace(log_, "manager_dispatcher_t::load session: {}", session.data());
        return actor_zeta::send(dispatcher(), address(), core::handler_id(core::route::load), session, current_message()->sender());
    }

    void manager_dispatcher_t::execute_ql(components::session::session_id_t& session, ql_statement_t* ql) {
        trace(log_, "manager_dispatcher_t::execute_ql session: {}, {}", session.data(), ql->to_string());
        return actor_zeta::send(dispatcher(), address(), handler_id(route::execute_ql), session, ql, current_message()->sender());
    }

    void manager_dispatcher_t::delete_documents(components::session::session_id_t& session, components::ql::ql_statement_t* statement) {
        trace(log_, "manager_dispatcher_t::delete_documents session: {}, database: {}, collection name: {} ", session.data(), statement->database_, statement->collection_);
        return actor_zeta::send(dispatcher(), address(), collection::handler_id(collection::route::delete_documents), session, std::move(statement), current_message()->sender());
    }

    void manager_dispatcher_t::update_documents(components::session::session_id_t& session, components::ql::ql_statement_t* statement) {
        trace(log_, "manager_dispatcher_t::update_documents session: {}, database: {}, collection name: {} ", session.data(), statement->database_, statement->collection_);
        return actor_zeta::send(dispatcher(), address(), collection::handler_id(collection::route::update_documents), session, std::move(statement), current_message()->sender());
    }

    void manager_dispatcher_t::size(components::session::session_id_t& session, std::string& database_name, std::string& collection) {
        trace(log_, "manager_dispatcher_t::size session: {} , database: {}, collection name: {} ", session.data(), database_name, collection);
        actor_zeta::send(dispatcher(), address(), collection::handler_id(collection::route::size), session, std::move(database_name), std::move(collection), current_message()->sender());
    }

    void manager_dispatcher_t::close_cursor(components::session::session_id_t&) {
    }

    void manager_dispatcher_t::create_index(components::session::session_id_t &session, components::ql::create_index_t index) {
        trace(log_, "manager_dispatcher_t::create_index session: {} , index: {}", session.data(), index.name());
        actor_zeta::send(dispatcher(), address(), collection::handler_id(collection::route::create_index), session, std::move(index), current_message()->sender());
    }

    void manager_dispatcher_t::drop_index(components::session::session_id_t& session, components::ql::drop_index_t drop_index) {
        trace(log_, "manager_dispatcher_t::drop_index session: {} , index: {}", session.data(), drop_index.name());
        actor_zeta::send(dispatcher(), address(), collection::handler_id(collection::route::drop_index), session, std::move(drop_index), current_message()->sender());
    }

    auto manager_dispatcher_t::dispatcher() -> actor_zeta::address_t {
        return dispatchers_[0]->address();
    }

} // namespace services::dispatcher
