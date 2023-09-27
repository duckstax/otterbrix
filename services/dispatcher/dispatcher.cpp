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
using namespace components::result;

namespace services::dispatcher {

    dispatcher_t::dispatcher_t(
        manager_dispatcher_t* manager_dispatcher,
        std::pmr::memory_resource *o_resource,
        actor_zeta::address_t mstorage,
        actor_zeta::address_t mwal,
        actor_zeta::address_t mdisk,
        log_t& log,
        std::string name)
        : actor_zeta::basic_actor<dispatcher_t>(manager_dispatcher)
        ,  load_(actor_zeta::make_behavior(resource(),core::handler_id(core::route::load),this, &dispatcher_t::load))
        ,  disk_load_finish_(actor_zeta::make_behavior(resource(),disk::handler_id(disk::route::load_finish),this, &dispatcher_t::load_from_disk_result))
        ,  memory_storage_load_finish_(actor_zeta::make_behavior(resource(),memory_storage::handler_id(memory_storage::route::load_finish),this, &dispatcher_t::load_from_memory_resource_result))
        ,  remove_collection_finish_(actor_zeta::make_behavior(resource(),disk::handler_id(disk::route::remove_collection_finish),this, &dispatcher_t::drop_collection_finish_from_disk))
        ,  wal_load_finish_(actor_zeta::make_behavior(resource(),wal::handler_id(wal::route::load_finish),this, &dispatcher_t::load_from_wal_result))
        ,  execute_ql_(actor_zeta::make_behavior(resource(),handler_id(route::execute_ql),this, &dispatcher_t::execute_ql))
        ,  execute_plan_finish_(actor_zeta::make_behavior(resource(),memory_storage::handler_id(memory_storage::route::execute_plan_finish),this, &dispatcher_t::execute_ql_finish))
        ,  insert_documents_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::insert_documents),this, &dispatcher_t::insert_documents))
        ,  insert_finish_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::insert_finish),this, &dispatcher_t::insert_finish))
        ,  delete_documents_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::delete_documents),this, &dispatcher_t::delete_documents))
        ,  delete_finish_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::delete_finish),this, &dispatcher_t::delete_finish))
        ,  update_documents_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::update_documents),this, &dispatcher_t::update_documents))
        ,  update_finish_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::update_finish),this, &dispatcher_t::update_finish))
        ,  size_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::size),this, &dispatcher_t::size))
        ,  size_finish_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::size_finish),this, &dispatcher_t::size_finish))
        ,  close_cursor_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::close_cursor),this, &dispatcher_t::close_cursor))
        ,  create_index_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::create_index),this, &dispatcher_t::create_index))
        ,  create_index_finish_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::create_index_finish),this, &dispatcher_t::create_index_finish))
        ,  drop_index_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::drop_index),this, &dispatcher_t::drop_index))
        ,  drop_index_finish_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::drop_index_finish),this, &dispatcher_t::drop_index_finish))
        ,  success_(actor_zeta::make_behavior(resource(),wal::handler_id(wal::route::success),this, &dispatcher_t::wal_success))
        , name_(std::move(name))
        , log_(log.clone())
        , resource_(o_resource)
        , manager_dispatcher_(manager_dispatcher->address())
        , memory_storage_(std::move(mstorage))
        , manager_wal_(std::move(mwal))
        , manager_disk_(std::move(mdisk)) {
        trace(log_, "dispatcher_t::dispatcher_t start name:{}", make_type());
    }

    dispatcher_t::~dispatcher_t() {
        trace(log_, "delete dispatcher_t");
    }

    auto dispatcher_t::make_type() const noexcept -> const char* const{
        return name_.c_str();
    }

    actor_zeta::behavior_t dispatcher_t::behavior() {
        return actor_zeta::make_behavior(
            resource(),
            [this](actor_zeta::message* msg) -> void {
                switch (msg->command()) {
                    case core::handler_id(core::route::load): {
                        load_(msg);
                        break;
                    }

                    case disk::handler_id(disk::route::load_finish): {
                        disk_load_finish_(msg);
                        break;
                    }
                    case memory_storage::handler_id(memory_storage::route::load_finish): {
                        memory_storage_load_finish_(msg);
                        break;
                    }
                    case disk::handler_id(disk::route::remove_collection_finish): {
                        disk_load_finish_(msg);
                        break;
                    }
                    case wal::handler_id(wal::route::load_finish): {
                        wal_load_finish_(msg);
                        break;
                    }
                    case handler_id(route::execute_ql): {
                        execute_ql_(msg);
                        break;
                    }
                    case memory_storage::handler_id(memory_storage::route::execute_plan_finish): {
                        execute_plan_finish_(msg);
                        break;
                    }
                    case collection::handler_id(collection::route::insert_documents): {
                        insert_documents_(msg);
                        break;
                    }
                    case collection::handler_id(collection::route::insert_finish): {
                        insert_finish_(msg);
                        break;
                    }
                    case collection::handler_id(collection::route::delete_documents): {
                        delete_documents_(msg);
                        break;
                    }
                    case collection::handler_id(collection::route::delete_finish): {
                        delete_finish_(msg);
                        break;
                    }
                    case collection::handler_id(collection::route::update_documents): {
                        update_documents_(msg);
                        break;
                    }
                    case collection::handler_id(collection::route::update_finish): {
                        update_finish_(msg);
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
                    case collection::handler_id(collection::route::create_index): {
                        create_index_(msg);
                        break;
                    }

                    case collection::handler_id(collection::route::create_index_finish): {
                        create_index_finish_(msg);
                        break;
                    }
                    case collection::handler_id(collection::route::drop_index): {
                        drop_index_(msg);
                        break;
                    }
                    case collection::handler_id(collection::route::drop_index_finish): {
                        drop_index_finish_(msg);
                        break;
                    }
                    case wal::handler_id(wal::route::success): {
                        success_(msg);
                        break;
                    }
                }
            });
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

    void dispatcher_t::load_from_memory_resource_result(components::session::session_id_t &session, const result_t &result) {
        trace(log_, "dispatcher_t::load_from_memory_resource_result, session: {}", session.data());
        const auto& collections = result.get<result_list_addresses_t>();
        for (const auto& collection : collections.addresses) {
            collection_address_book_.emplace(collection.name, collection.address);
        }
        actor_zeta::send(manager_disk_, address(), disk::handler_id(disk::route::load_indexes), session);
        actor_zeta::send(manager_wal_, address(), wal::handler_id(wal::route::load), session, load_result_.wal_id());
        load_result_.clear();
    }

    void dispatcher_t::load_from_wal_result(components::session::session_id_t& session, std::vector<services::wal::record_t> &records) {
        load_count_answers_ = records.size();
        trace(log_, "dispatcher_t::load_from_wal_result, session: {}, count commands: {}", session.data(), load_count_answers_);
        if (load_count_answers_ == 0) {
            actor_zeta::send(find_session(session_to_address_, session).address(), dispatcher_t::address(), core::handler_id(core::route::load_finish));
            remove_session(session_to_address_, session);
            return;
        }
        last_wal_id_ = records[load_count_answers_ - 1].id;
        for (const auto &record : records) {
            switch (record.type) {
                case statement_type::create_database: {
                    auto data = std::get<create_database_t>(record.data);
                    components::session::session_id_t session_database;
                    execute_ql(session_database, &data, manager_wal_);
                    break;
                }
                    case statement_type::drop_database: {
                    auto data = std::get<drop_database_t>(record.data);
                    components::session::session_id_t session_database;
                    execute_ql(session_database, &data, manager_wal_);
                    break;
                }
                    case statement_type::create_collection: {
                    auto data = std::get<create_collection_t>(record.data);
                    components::session::session_id_t session_collection;
                    execute_ql(session_collection, &data, manager_wal_);
                    break;
                }
                case statement_type::drop_collection: {
                    auto data = std::get<drop_collection_t>(record.data);
                    components::session::session_id_t session_collection;
                    execute_ql(session_collection, &data, manager_wal_);
                    break;
                }
                case statement_type::insert_one: {
                    auto ql = std::get<insert_one_t>(record.data);
                    components::session::session_id_t session_insert;
                    insert_documents(session_insert, &ql, manager_wal_);
                    break;
                }
                case statement_type::insert_many: {
                    auto ql = std::get<insert_many_t>(record.data);
                    components::session::session_id_t session_insert;
                    insert_documents(session_insert, &ql, manager_wal_);
                    break;
                }
                case statement_type::delete_one: {
                    auto ql = std::get<delete_one_t>(record.data);
                    components::session::session_id_t session_delete;
                    delete_documents(session_delete, &ql, manager_wal_);
                    break;
                }
                case statement_type::delete_many: {
                    auto ql = std::get<delete_many_t>(record.data);
                    components::session::session_id_t session_delete;
                    delete_documents(session_delete, &ql, manager_wal_);
                    break;
                }
                case statement_type::update_one: {
                    auto ql = std::get<update_one_t>(record.data);
                    components::session::session_id_t session_update;
                    update_documents(session_update, &ql, manager_wal_);
                    break;
                }
                case statement_type::update_many: {
                    auto ql = std::get<update_many_t>(record.data);
                    components::session::session_id_t session_update;
                    update_documents(session_update, &ql, manager_wal_);
                    break;
                }
                case statement_type::create_index: {
                    auto data = std::get<create_index_t>(record.data);
                    components::session::session_id_t session_create_index;
                    create_index(session_create_index, std::move(data), manager_wal_);
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

    void dispatcher_t::execute_ql_finish(components::session::session_id_t& session, const result_t& result) {
        auto& s = find_session(session_to_address_, session);
        auto* ql = s.get<ql_statement_t*>();
        trace(log_, "dispatcher_t::execute_ql_finish: session {}, {}, {}", session.data(), ql->to_string(), result.is_success());
        if (result.is_success()) {
            //todo: delete

            if (ql->type() == statement_type::create_database) {
                actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::append_database), session, ql->database_);
                if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
                    wal_success(session, last_wal_id_);
                } else {
                    actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::create_database), session, *static_cast<create_database_t*>(ql));
                }
            }

            if (ql->type() == statement_type::create_collection) {
                collection_address_book_.emplace(collection_full_name_t(ql->database_, ql->collection_), result.get<result_address_t>().address);
                actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::append_collection), session, ql->database_, ql->collection_);
                if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
                    wal_success(session, last_wal_id_);
                } else {
                    actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::create_collection), session, *static_cast<create_collection_t*>(ql));
                }
            }

            if (ql->type() == statement_type::drop_collection) {
                collection_full_name_t name(ql->database_, ql->collection_);
                collection_address_book_.erase(name);
                actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::remove_collection), session, ql->database_, ql->collection_);
                if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
                    wal_success(session, last_wal_id_);
                } else {
                    actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::drop_collection), session, components::ql::drop_collection_t(ql->database_, ql->collection_));
                }
                return;
            }

            //end: delete
        }
        if (!check_load_from_wal(session)) {
            actor_zeta::send(s.address(), dispatcher_t::address(), handler_id(route::execute_ql_finish), session, result);
            remove_session(session_to_address_, session);
        }
    }

    void dispatcher_t::drop_collection_finish_from_disk(components::session::session_id_t& session, std::string& collection_name) {
        trace(log_, "dispatcher_t::drop_collection_finish_from_disk: {}", collection_name);
        if (!check_load_from_wal(session)) {
            auto address = find_session(session_to_address_, session).address();
            remove_session(session_to_address_, session);
            actor_zeta::send(address, dispatcher_t::address(), handler_id(route::execute_ql_finish), session,
                             components::result::make_result(components::result::empty_result_t()));
        }
    }

    void dispatcher_t::insert_documents(components::session::session_id_t& session, ql_statement_t* statement, actor_zeta::address_t address) {
        trace(log_, "dispatcher_t::insert: session:{}, database: {}, collection: {}", session.data(), statement->database_, statement->collection_);
        collection_full_name_t key{statement->database_, statement->collection_};
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            if (statement->type() == statement_type::insert_one) {
                make_session(session_to_address_, session, session_t(std::move(address), *static_cast<insert_one_t*>(statement)));
            } else {
                make_session(session_to_address_, session, session_t(std::move(address), *static_cast<insert_many_t*>(statement)));
            }
            auto logic_plan = create_logic_plan(statement).first;
            actor_zeta::send(it_collection->second, dispatcher_t::address(), collection::handler_id(collection::route::insert_documents), session, logic_plan, components::ql::storage_parameters{});
        } else {
            actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::insert_finish), session, result_insert(resource_));
        }
    }

    void dispatcher_t::insert_finish(components::session::session_id_t& session, result_insert& result) {
        trace(log_, "dispatcher_t::insert_finish session: {}", session.data());
        auto& s = find_session(session_to_address_, session);
        if (s.address().get() == manager_wal_.get()) {
            wal_success(session, last_wal_id_);
        } else {
            if (s.type() == statement_type::insert_one) {
                actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::insert_one), session, s.get<insert_one_t>());
            } else {
                actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::insert_many), session, s.get<insert_many_t>());
            }
        }
        if (!check_load_from_wal(session)) {
            actor_zeta::send(s.address(), dispatcher_t::address(), collection::handler_id(collection::route::insert_finish), session, result);
            remove_session(session_to_address_, session);
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
            actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::delete_finish), session, result_delete(resource_));
        }
    }

    void dispatcher_t::delete_finish(components::session::session_id_t& session, result_delete& result) {
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
            actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::update_finish), session, result_update(resource_));
        }
    }

    void dispatcher_t::update_finish(components::session::session_id_t& session, result_update& result) {
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
            actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::size_finish), session, result_size());
        }
    }

    void dispatcher_t::size_finish(components::session::session_id_t& session, result_size& result) {
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
                actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::create_index_finish), session, result_create_index());
            }
        }
    }

    void dispatcher_t::create_index_finish(components::session::session_id_t &session, const std::string& name, result_create_index& result) {
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
                actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::drop_index_finish), session, result_drop_index());
            }
        }
    }

    void dispatcher_t::drop_index_finish(components::session::session_id_t &session, const std::string& name, result_drop_index& result) {
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
        actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::flush), session, wal_id);
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
        std::pmr::memory_resource* resource,
        actor_zeta::scheduler_raw scheduler,
        log_t& log)
        : actor_zeta::cooperative_supervisor<manager_dispatcher_t>(resource)
        , create_(actor_zeta::make_behavior(resource(),handler_id(route::create), this, &manager_dispatcher_t::create))
        , load_(actor_zeta::make_behavior(resource(),core::handler_id(core::route::load), this, &manager_dispatcher_t::load))
        , execute_ql_(actor_zeta::make_behavior(resource(),handler_id(route::execute_ql), this, &manager_dispatcher_t::execute_ql))
        , insert_documents_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::insert_documents), this, &manager_dispatcher_t::insert_documents))
        , delete_documents_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::delete_documents), this, &manager_dispatcher_t::delete_documents))
        , update_documents_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::update_documents), this, &manager_dispatcher_t::update_documents))
        , size_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::size), this, &manager_dispatcher_t::size))
        , close_cursor_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::close_cursor), this, &manager_dispatcher_t::close_cursor))
        , create_index_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::create_index), this, &manager_dispatcher_t::create_index))
        , drop_index_(actor_zeta::make_behavior(resource(),collection::handler_id(collection::route::drop_index), this, &manager_dispatcher_t::drop_index))
        , sync_(actor_zeta::make_behavior(resource(),core::handler_id(core::route::sync), this, &manager_dispatcher_t::sync))
        , log_(log.clone())
        , e_(scheduler) {
    }

    manager_dispatcher_t::~manager_dispatcher_t() {
        ZoneScoped;
        trace(log_, "delete manager_dispatcher_t");
    }

    //NOTE: behold thread-safety!
    auto manager_dispatcher_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        ZoneScoped;
        std::unique_lock<spin_lock> _(lock_);
        set_current_message(std::move(msg));
        behavior()(current_message());
    }

    auto manager_dispatcher_t::make_type() const noexcept -> const char* const {
        return "manager_dispatcher";
    }

    auto manager_dispatcher_t::make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t* {
        return e_;
    }

    actor_zeta::behavior_t manager_dispatcher_t::behavior() {
        return actor_zeta::make_behavior(
            resource(),
            [this](actor_zeta::message* msg) -> void {
                switch (msg->command()) {
                    case handler_id(route::create): {
                        create_(msg);
                        break;
                    }
                    case core::handler_id(core::route::load): {
                        load_(msg);
                        break;
                    }
                    case handler_id(route::execute_ql): {
                        execute_ql_(msg);
                        break;
                    }
                    case collection::handler_id(collection::route::insert_documents): {
                        insert_documents_(msg);
                        break;
                    }
                    case collection::handler_id(collection::route::delete_documents): {
                        delete_documents_(msg);
                        break;
                    }
                    case collection::handler_id(collection::route::update_documents): {
                        update_documents_(msg);
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
                    case collection::handler_id(collection::route::create_index): {
                        create_index_(msg);
                        break;
                    }
                    case collection::handler_id(collection::route::drop_index): {
                        drop_index_(msg);
                        break;
                    }
                    case core::handler_id(core::route::sync):{
                        break;
                    }
                }
            });
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

    void manager_dispatcher_t::insert_documents(components::session::session_id_t& session, components::ql::ql_statement_t* statement) {
        trace(log_, "manager_dispatcher_t::insert_documents session: {}, database: {}, collection name: {} ", session.data(), statement->database_, statement->collection_);
        return actor_zeta::send(dispatcher(), address(), collection::handler_id(collection::route::insert_documents), session, std::move(statement), current_message()->sender());
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

    void manager_dispatcher_t::close_cursor(components::session::session_id_t&) {}

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
