#include "dispatcher.hpp"
#include "parser/parser.hpp"

#include <core/tracy/tracy.hpp>
#include <core/system_command.hpp>

#include <components/document/document.hpp>
#include <components/protocol/protocol.hpp>

#include <services/collection/route.hpp>
#include <services/database/database.hpp>
#include <services/database/route.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/disk/route.hpp>
#include <services/wal/route.hpp>

namespace services::dispatcher {

    key_collection_t::key_collection_t(const database_name_t& database, const collection_name_t & collection)
        : database_(database)
        , collection_(collection) {
    }

    const database_name_t& key_collection_t::database() const {
        return database_;
    }

    const collection_name_t& key_collection_t::collection() const {
        return collection_;
    }

    bool key_collection_t::operator==(const key_collection_t& other) const {
        return database_ == other.database_ && collection_ == other.collection_;
    }

    std::size_t key_collection_t::hash::operator()(const key_collection_t& key) const {
        return std::hash<std::string>()(key.database()) ^ std::hash<std::string>()(key.collection());
    }


    dispatcher_t::dispatcher_t(
        manager_dispatcher_t* manager_dispatcher,
        actor_zeta::address_t mdb,
        actor_zeta::address_t mwal,
        actor_zeta::address_t mdisk,
        log_t& log,
        std::string name)
        : actor_zeta::basic_async_actor(manager_dispatcher, std::move(name))
        , log_(log.clone())
        , manager_dispatcher_(manager_dispatcher->address())
        , manager_database_(mdb)
        , manager_wal_(mwal)
        , manager_disk_(mdisk) {
        trace(log_, "dispatcher_t::dispatcher_t start name:{}", type());
        add_handler(core::handler_id(core::route::load), &dispatcher_t::load);
        add_handler(disk::handler_id(disk::route::load_finish), &dispatcher_t::load_from_disk_result);
        add_handler(database::handler_id(database::route::create_database), &dispatcher_t::create_database);
        add_handler(database::handler_id(database::route::create_database_finish), &dispatcher_t::create_database_finish);
        add_handler(database::handler_id(database::route::create_collection), &dispatcher_t::create_collection);
        add_handler(database::handler_id(database::route::create_collection_finish), &dispatcher_t::create_collection_finish);
        add_handler(database::handler_id(database::route::drop_collection), &dispatcher_t::drop_collection);
        add_handler(database::handler_id(database::route::drop_collection_finish), &dispatcher_t::drop_collection_finish);
        add_handler(collection::handler_id(collection::route::drop_collection_finish), &dispatcher_t::drop_collection_finish_collection);
        add_handler(collection::handler_id(collection::route::insert_one), &dispatcher_t::insert_one);
        add_handler(collection::handler_id(collection::route::insert_many), &dispatcher_t::insert_many);
        add_handler(collection::handler_id(collection::route::insert_one_finish), &dispatcher_t::insert_one_finish);
        add_handler(collection::handler_id(collection::route::insert_many_finish), &dispatcher_t::insert_many_finish);
        add_handler(collection::handler_id(collection::route::find), &dispatcher_t::find);
        add_handler(collection::handler_id(collection::route::find_finish), &dispatcher_t::find_finish);
        add_handler(collection::handler_id(collection::route::find_one), &dispatcher_t::find_one);
        add_handler(collection::handler_id(collection::route::find_one_finish), &dispatcher_t::find_one_finish);
        add_handler(collection::handler_id(collection::route::delete_one), &dispatcher_t::delete_one);
        add_handler(collection::handler_id(collection::route::delete_many), &dispatcher_t::delete_many);
        add_handler(collection::handler_id(collection::route::delete_finish), &dispatcher_t::delete_finish);
        add_handler(collection::handler_id(collection::route::update_one), &dispatcher_t::update_one);
        add_handler(collection::handler_id(collection::route::update_many), &dispatcher_t::update_many);
        add_handler(collection::handler_id(collection::route::update_finish), &dispatcher_t::update_finish);
        add_handler(collection::handler_id(collection::route::size), &dispatcher_t::size);
        add_handler(collection::handler_id(collection::route::size_finish), &dispatcher_t::size_finish);
        add_handler(collection::handler_id(collection::route::close_cursor), &dispatcher_t::close_cursor);
        add_handler(wal::handler_id(wal::route::success), &dispatcher_t::wal_success);
        trace(log_, "dispatcher_t::dispatcher_t finish name:{}", type());
    }

    void dispatcher_t::load(components::session::session_id_t &session, actor_zeta::address_t sender) {
        trace(log_, "dispatcher_t::load: session {}", session.data());
        session_to_address_.emplace(session, sender);
        actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::load), session);
    }

    void dispatcher_t::load_from_disk_result(components::session::session_id_t &session, const disk::result_load_t &result) {
        trace(log_, "dispatcher_t::load_from_disk_result: session {}", session.data());
        for (const auto &database : *result) {
            trace(log_, "___{}", database.name);
        }
        actor_zeta::send(session_to_address_.at(session).address(), dispatcher_t::address(), core::handler_id(core::route::load_finish));
        session_to_address_.erase(session);
    }

    void dispatcher_t::create_database(components::session::session_id_t& session, std::string& name, actor_zeta::address_t address) {
        trace(log_, "dispatcher_t::create_database: session {} , name create_database {}", session.data(), name);
        session_to_address_.emplace(session, address);
        actor_zeta::send(manager_database_, dispatcher_t::address(), database::handler_id(database::route::create_database), session, std::move(name));
    }

    void dispatcher_t::create_database_finish(components::session::session_id_t& session, database::database_create_result result, std::string& database_name, actor_zeta::address_t database) {
        trace(log_, "dispatcher_t::create_database: session {} , name create_database {}", session.data(), database_name);
        if (result.created_) {
            /// auto md = manager_dispatcher_;
            ///goblin_engineer::link(md, database);
            database_address_book_.emplace(database_name, database);
            actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::append_database), session, std::string(database_name));
            actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::create_database), session, components::protocol::create_database_t(database_name));
            trace(log_, "add database_create_result");
        }
        actor_zeta::send(session_to_address_.at(session).address(), dispatcher_t::address(), database::handler_id(database::route::create_database_finish), session, result);
        session_to_address_.erase(session);
    }

    void dispatcher_t::create_collection(components::session::session_id_t& session, std::string& database_name, std::string& collections_name, actor_zeta::address_t address) {
        trace(log_, "dispatcher_t::create_collection: session {} , database_name {} , collection_name {}", session.data(), database_name, collections_name);
        session_to_address_.emplace(session, address);
        actor_zeta::send(database_address_book_.at(database_name), dispatcher_t::address(), database::handler_id(database::route::create_collection), session, collections_name, manager_disk_);
    }

    void dispatcher_t::create_collection_finish(components::session::session_id_t& session, database::collection_create_result result, std::string& database_name, std::string& collection_name,actor_zeta::address_t collection) {
        trace(log_, "create_collection_finish: {}", collection_name);
        if (result.created_) {
            /// auto md = manager_dispatcher_;
            //goblin_engineer::link(md, collection);
            collection_address_book_.emplace(key_collection_t(database_name, std::string(collection_name)), collection);
            actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::append_collection), session, database_name, std::string(collection_name));
            actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::create_collection), session, components::protocol::create_collection_t(database_name, collection_name));
            trace(log_, "add database_create_result");
        }
        actor_zeta::send(session_to_address_.at(session).address(), dispatcher_t::address(), database::handler_id(database::route::create_collection_finish), session, result);
        session_to_address_.erase(session);
    }

    void dispatcher_t::drop_collection(components::session::session_id_t& session, std::string& database_name, std::string& collection_name, actor_zeta::address_t address) {
        trace(log_, "dispatcher_t::drop_collection: session {} , database_name {} , collection_name {}", session.data(), database_name, collection_name);
        auto it_collection = collection_address_book_.find({database_name, collection_name});
        if (it_collection != collection_address_book_.end()) {
            session_to_address_.emplace(session, address);
            actor_zeta::send(it_collection->second, dispatcher_t::address(), collection::handler_id(collection::route::drop_collection), session);
        } else {
            actor_zeta::send(address, dispatcher_t::address(), database::handler_id(database::route::drop_collection_finish), session, result_drop_collection(false));
        }
    }

    void dispatcher_t::drop_collection_finish_collection(components::session::session_id_t& session, result_drop_collection& result, std::string& database_name, std::string& collection_name) {
        trace(log_, "dispatcher_t::drop_collection_finish_collection: database_name {} , collection_name {}, result: {}", session.data(), database_name, collection_name, result.is_success());
        if (result.is_success()) {
            actor_zeta::send(database_address_book_.at(database_name), dispatcher_t::address(), database::handler_id(database::route::drop_collection), session, collection_name);
        } else {
            actor_zeta::send(session_to_address_.at(session).address(), dispatcher_t::address(), database::handler_id(database::route::drop_collection_finish), session, result);
            session_to_address_.erase(session);
        }
    }

    void dispatcher_t::drop_collection_finish(components::session::session_id_t& session, result_drop_collection& result, std::string& database_name, std::string& collection_name, actor_zeta::address_t collection) {
        trace(log_, "drop_collection_finish: {}", collection_name);
        if (result.is_success()) {
            ///auto md = address_book("manager_dispatcher");
            ///goblin_engineer::link(md, collection);
            collection_address_book_.erase({database_name, std::string(collection_name)});
            actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::remove_collection), session, database_name, std::string(collection_name));
            actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::drop_collection), session, components::protocol::drop_collection_t(database_name, collection_name));
            trace(log_, "collection {} dropped", collection_name);
        }
        actor_zeta::send(session_to_address_.at(session).address(), dispatcher_t::address(), database::handler_id(database::route::drop_collection_finish), session, result);
        session_to_address_.erase(session);
    }

    void dispatcher_t::insert_one(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& document, actor_zeta::address_t address) {
        debug(log_, "dispatcher_t::insert_one: session:{}, database: {}, collection: {}", session.data(), database_name, collection);
        key_collection_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            make_session(session_to_address_, session, session_t(address, insert_one_t(database_name, collection, document)));
            actor_zeta::send(it_collection->second, dispatcher_t::address(), collection::handler_id(collection::route::insert_one), session, std::move(document));
        } else {
            actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::insert_one_finish), session, result_insert_one());
        }
    }

    void dispatcher_t::insert_many(components::session::session_id_t& session, std::string& database_name, std::string& collection, std::list<components::document::document_ptr>& documents, actor_zeta::address_t address) {
        debug(log_, "dispatcher_t::insert_many: session:{}, database: {}, collection: {}", session.data(), database_name, collection);
        key_collection_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            make_session(session_to_address_, session, session_t(address, insert_many_t(database_name, collection, documents)));
            actor_zeta::send(it_collection->second, dispatcher_t::address(), collection::handler_id(collection::route::insert_many), session, std::move(documents));
        } else {
            actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::insert_many_finish), session, result_insert_many());
        }
    }

    void dispatcher_t::insert_one_finish(components::session::session_id_t& session, result_insert_one& result) {
        trace(log_, "dispatcher_t::insert_one_finish session: {}", session.data());
        auto& s = ::find(session_to_address_, session);
        actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::insert_one), session, s.get<insert_one_t>());
        actor_zeta::send(session_to_address_.at(session).address(), dispatcher_t::address(), collection::handler_id(collection::route::insert_one_finish), session, result);
        session_to_address_.erase(session);
    }

    void dispatcher_t::insert_many_finish(components::session::session_id_t& session, result_insert_many& result) {
        trace(log_, "dispatcher_t::insert_many_finish session: {}", session.data());
        auto& s = ::find(session_to_address_, session);
        actor_zeta::send(manager_wal_, dispatcher_t::address(), wal::handler_id(wal::route::insert_many), session, s.get<insert_many_t>());
        actor_zeta::send(s.address(), dispatcher_t::address(), collection::handler_id(collection::route::insert_many_finish), session, result);
        ::remove(session_to_address_, session);
    }

    void dispatcher_t::find(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition, actor_zeta::address_t address) {
        debug(log_, "dispatcher_t::find: session:{}, database: {}, collection: {}", session.data(), database_name, collection);

        key_collection_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            session_to_address_.emplace(session, address);
            actor_zeta::send(it_collection->second, dispatcher_t::address(), collection::handler_id(collection::route::find), session, components::parser::parse_find_condition(condition));
        } else {
            actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::find_finish), session, result_find());
        }
    }

    void dispatcher_t::find_finish(components::session::session_id_t& session, components::cursor::sub_cursor_t* cursor) {
        trace(log_, "dispatcher_t::find_finish session: {}", session.data());
        auto result = new components::cursor::cursor_t();
        if (cursor) {
            result->push(cursor);
        }
        actor_zeta::send(session_to_address_.at(session).address(), dispatcher_t::address(), collection::handler_id(collection::route::find_finish), session, result);
        session_to_address_.erase(session);
    }

    void dispatcher_t::find_one(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition, actor_zeta::address_t address) {
        debug(log_, "dispatcher_t::find_one: session:{}, database: {}, collection: {}", session.data(), database_name, collection);
        key_collection_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            session_to_address_.emplace(session, address);
            actor_zeta::send(it_collection->second, dispatcher_t::address(), collection::handler_id(collection::route::find_one), session, components::parser::parse_find_condition(condition));
        } else {
            actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::find_one_finish), session, result_find_one());
        }
    }

    void dispatcher_t::find_one_finish(components::session::session_id_t& session, result_find_one& result) {
        trace(log_, "dispatcher_t::find_one_finish session: {}", session.data());
        actor_zeta::send(session_to_address_.at(session).address(), dispatcher_t::address(), collection::handler_id(collection::route::find_one_finish), session, result);
        session_to_address_.erase(session);
    }

    void dispatcher_t::delete_one(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition, actor_zeta::address_t address) {
        debug(log_, "dispatcher_t::delete_one: session:{}, database: {}, collection: {}", session.data(), database_name, collection);
        key_collection_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            session_to_address_.emplace(session, address);
            actor_zeta::send(it_collection->second, dispatcher_t::address(), collection::handler_id(collection::route::delete_one), session, components::parser::parse_find_condition(condition));
        } else {
            actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::delete_finish), session, result_delete());
        }
    }

    void dispatcher_t::delete_many(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition, actor_zeta::address_t address) {
        debug(log_, "dispatcher_t::delete_many: session:{}, database: {}, collection: {}", session.data(), database_name, collection);
        key_collection_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            session_to_address_.emplace(session, address);
            actor_zeta::send(it_collection->second, dispatcher_t::address(), collection::handler_id(collection::route::delete_many), session, components::parser::parse_find_condition(condition));
        } else {
            actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::delete_finish), session, result_delete());
        }
    }

    void dispatcher_t::delete_finish(components::session::session_id_t& session, result_delete& result) {
        trace(log_, "dispatcher_t::delete_finish session: {}", session.data());
        actor_zeta::send(session_to_address_.at(session).address(), dispatcher_t::address(), collection::handler_id(collection::route::delete_finish), session, result);
        if (!result.empty()) {
            actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::flush), session, wal::id_t()); //todo after wal
        }
        session_to_address_.erase(session);
    }

    void dispatcher_t::update_one(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition, components::document::document_ptr& update, bool upsert, actor_zeta::address_t address) {
        debug(log_, "dispatcher_t::update_one: session:{}, database: {}, collection: {}", session.data(), database_name, collection);
        key_collection_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            session_to_address_.emplace(session, address);
            actor_zeta::send(it_collection->second, dispatcher_t::address(), collection::handler_id(collection::route::update_one), session, components::parser::parse_find_condition(condition), std::move(update), upsert);
        } else {
            actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::update_finish), session, result_update());
        }
    }

    void dispatcher_t::update_many(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition, components::document::document_ptr& update, bool upsert, actor_zeta::address_t address) {
        debug(log_, "dispatcher_t::update_many: session:{}, database: {}, collection: {}", session.data(), database_name, collection);
        key_collection_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            session_to_address_.emplace(session, address);
            actor_zeta::send(it_collection->second, dispatcher_t::address(), collection::handler_id(collection::route::update_many), session, components::parser::parse_find_condition(condition), std::move(update), upsert);
        } else {
            actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::update_finish), session, result_update());
        }
    }

    void dispatcher_t::update_finish(components::session::session_id_t& session, result_update& result) {
        trace(log_, "dispatcher_t::update_finish session: {}", session.data());
        actor_zeta::send(session_to_address_.at(session).address(), dispatcher_t::address(), collection::handler_id(collection::route::update_finish), session, result);
        if (!result.empty()) {
            actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::flush), session, wal::id_t()); //todo after wal
        }
        session_to_address_.erase(session);
    }

    void dispatcher_t::size(components::session::session_id_t& session, std::string& database_name, std::string& collection, actor_zeta::address_t address) {
        trace(log_, "dispatcher_t::size: session:{}, database: {}, collection: {}", session.data(), database_name, collection);
        key_collection_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            session_to_address_.emplace(session, address);
            actor_zeta::send(it_collection->second, dispatcher_t::address(), collection::handler_id(collection::route::size), session);
        } else {
            actor_zeta::send(address, dispatcher_t::address(), collection::handler_id(collection::route::size_finish), session, result_size());
        }
    }

    void dispatcher_t::size_finish(components::session::session_id_t& session, result_size& result) {
        trace(log_, "dispatcher_t::size_finish session: {}", session.data());
        actor_zeta::send(session_to_address_.at(session).address(), dispatcher_t::address(), collection::handler_id(collection::route::size_finish), session, result);
        session_to_address_.erase(session);
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
        add_handler(database::handler_id(database::route::create_database), &manager_dispatcher_t::create_database);
        add_handler(database::handler_id(database::route::create_collection), &manager_dispatcher_t::create_collection);
        add_handler(database::handler_id(database::route::drop_collection), &manager_dispatcher_t::drop_collection);
        add_handler(collection::handler_id(collection::route::insert_one), &manager_dispatcher_t::insert_one);
        add_handler(collection::handler_id(collection::route::insert_many), &manager_dispatcher_t::insert_many);
        add_handler(collection::handler_id(collection::route::find), &manager_dispatcher_t::find);
        add_handler(collection::handler_id(collection::route::find_one), &manager_dispatcher_t::find_one);
        add_handler(collection::handler_id(collection::route::delete_one), &manager_dispatcher_t::delete_one);
        add_handler(collection::handler_id(collection::route::delete_many), &manager_dispatcher_t::delete_many);
        add_handler(collection::handler_id(collection::route::update_one), &manager_dispatcher_t::update_one);
        add_handler(collection::handler_id(collection::route::update_many), &manager_dispatcher_t::update_many);
        add_handler(collection::handler_id(collection::route::size), &manager_dispatcher_t::size);
        add_handler(collection::handler_id(collection::route::close_cursor), &manager_dispatcher_t::close_cursor);
        add_handler(core::handler_id(core::route::sync), &manager_dispatcher_t::sync);
        trace(log_, "manager_dispatcher_t finish");
    }

    manager_dispatcher_t::~manager_dispatcher_t() {
        ZoneScoped;
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
            manager_database_, manager_wal_, manager_disk_, log_, std::string(name));
    }

    void manager_dispatcher_t::load(components::session::session_id_t &session) {
        trace(log_, "manager_dispatcher_t::load session: {}", session.data());
        return actor_zeta::send(dispatcher(), address(), core::handler_id(core::route::load), session, current_message()->sender());
    }

    void manager_dispatcher_t::create_database(components::session::session_id_t& session, std::string& name) {
        trace(log_, "manager_dispatcher_t::create_database session: {} , name: {} ", session.data(), name);
        return actor_zeta::send(dispatcher(), address(), database::handler_id(database::route::create_database), session, std::move(name), current_message()->sender());
    }

    void manager_dispatcher_t::create_collection(components::session::session_id_t& session, std::string& database_name, std::string& collection_name) {
        trace(log_, "manager_dispatcher_t::create_collection session: {} , database name: {} , collection name: {} ", session.data(), database_name, collection_name);
        return actor_zeta::send(dispatcher(), address(), database::handler_id(database::route::create_collection), session, std::move(database_name), std::move(collection_name), current_message()->sender());
    }

    void manager_dispatcher_t::drop_collection(components::session::session_id_t& session, std::string& database_name, std::string& collection_name) {
        trace(log_, "manager_dispatcher_t::drop_collection session: {} , database name: {} , collection name: {} ", session.data(), database_name, collection_name);
        return actor_zeta::send(dispatcher(), address(), database::handler_id(database::route::drop_collection), session, std::move(database_name), std::move(collection_name), current_message()->sender());
    }

    void manager_dispatcher_t::insert_one(components::session::session_id_t& session, std::string& database_name, std::string& collection_name, components::document::document_ptr& document) {
        trace(log_, "manager_dispatcher_t::insert_one session: {}, database: {}, collection name: {} ", session.data(), database_name, collection_name);
        return actor_zeta::send(dispatcher(), address(), collection::handler_id(collection::route::insert_one), session, std::move(database_name), std::move(collection_name), std::move(document), current_message()->sender());
    }

    void manager_dispatcher_t::insert_many(components::session::session_id_t& session, std::string& database_name, std::string& collection_name, std::list<components::document::document_ptr>& documents) {
        trace(log_, "manager_dispatcher_t::insert_many session: {}, database: {}, collection name: {} ", session.data(), database_name, collection_name);
        return actor_zeta::send(dispatcher(), address(), collection::handler_id(collection::route::insert_many), session, std::move(database_name), std::move(collection_name), std::move(documents), current_message()->sender());
    }

    void manager_dispatcher_t::find(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition) {
        trace(log_, "manager_dispatcher_t::find session: {}, database: {}, collection name: {} ", session.data(), database_name, collection);
        return actor_zeta::send(dispatcher(), address(), collection::handler_id(collection::route::find), session, std::move(database_name), std::move(collection), std::move(condition), current_message()->sender());
    }

    void manager_dispatcher_t::find_one(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition) {
        trace(log_, "manager_dispatcher_t::find_one session: {}, database: {}, collection name: {} ", session.data(), database_name, collection);
        return actor_zeta::send(dispatcher(), address(), collection::handler_id(collection::route::find_one), session, std::move(database_name), std::move(collection), std::move(condition), current_message()->sender());
    }

    void manager_dispatcher_t::delete_one(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition) {
        trace(log_, "manager_dispatcher_t::delete_one session: {}, database: {}, collection name: {} ", session.data(), database_name, collection);
        return actor_zeta::send(dispatcher(), address(), collection::handler_id(collection::route::delete_one), session, std::move(database_name), std::move(collection), std::move(condition), current_message()->sender());
    }

    void manager_dispatcher_t::delete_many(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition) {
        trace(log_, "manager_dispatcher_t::delete_many session: {}, database: {}, collection name: {} ", session.data(), database_name, collection);
        return actor_zeta::send(dispatcher(), address(), collection::handler_id(collection::route::delete_many), session, std::move(database_name), std::move(collection), std::move(condition), current_message()->sender());
    }

    void manager_dispatcher_t::update_one(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition, components::document::document_ptr& update, bool upsert) {
        trace(log_, "manager_dispatcher_t::update_one session: {}, database: {}, collection name: {} ", session.data(), database_name, collection);
        return actor_zeta::send(dispatcher(), address(), collection::handler_id(collection::route::update_one), session, std::move(database_name), std::move(collection), std::move(condition), std::move(update), upsert, current_message()->sender());
    }

    void manager_dispatcher_t::update_many(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition, components::document::document_ptr& update, bool upsert) {
        trace(log_, "manager_dispatcher_t::update_many session: {}, database: {}, collection name: {} ", session.data(), database_name, collection);
        return actor_zeta::send(dispatcher(), address(), collection::handler_id(collection::route::update_many), session, std::move(database_name), std::move(collection), std::move(condition), std::move(update), upsert, current_message()->sender());
    }

    void manager_dispatcher_t::size(components::session::session_id_t& session, std::string& database_name, std::string& collection) {
        trace(log_, "manager_dispatcher_t::size session: {} , database: {}, collection name: {} ", session.data(), database_name, collection);
        actor_zeta::send(dispatcher(), address(), collection::handler_id(collection::route::size), session, std::move(database_name), std::move(collection), current_message()->sender());
    }

    void manager_dispatcher_t::close_cursor(components::session::session_id_t& session) {
    }

    auto manager_dispatcher_t::dispatcher() -> actor_zeta::address_t {
        return dispatchers_[0]->address();
    }

} // namespace services::dispatcher