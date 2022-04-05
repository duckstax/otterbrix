#include "dispatcher.hpp"
#include "components/tracy/tracy.hpp"
#include "parser/parser.hpp"
#include "route.hpp"
#include <components/document/document.hpp>
#include <services/collection/route.hpp>
#include <services/database/database.hpp>
#include <services/disk/route.hpp>

namespace services::dispatcher {

    manager_dispatcher_t::manager_dispatcher_t(log_t& log, size_t num_workers, size_t max_throughput)
        : manager("manager_dispatcher")
        , log_(log.clone())
        , e_(new goblin_engineer::shared_work(num_workers, max_throughput), goblin_engineer::detail::thread_pool_deleter()) {
        ZoneScoped;
        log_.trace("manager_dispatcher_t::manager_dispatcher_t num_workers : {} , max_throughput: {}", num_workers, max_throughput);
        add_handler("create", &manager_dispatcher_t::create);
        add_handler("connect_me", &manager_dispatcher_t::connect_me);
        add_handler(manager_database::create_database, &manager_dispatcher_t::create_database);
        add_handler(database::create_collection, &manager_dispatcher_t::create_collection);
        add_handler(database::drop_collection, &manager_dispatcher_t::drop_collection);
        add_handler(collection::insert_one, &manager_dispatcher_t::insert_one);
        add_handler(collection::insert_many, &manager_dispatcher_t::insert_many);
        add_handler(collection::find, &manager_dispatcher_t::find);
        add_handler(collection::find_one, &manager_dispatcher_t::find_one);
        add_handler(collection::delete_one, &manager_dispatcher_t::delete_one);
        add_handler(collection::delete_many, &manager_dispatcher_t::delete_many);
        add_handler(collection::update_one, &manager_dispatcher_t::update_one);
        add_handler(collection::update_many, &manager_dispatcher_t::update_many);
        add_handler(collection::size, &manager_dispatcher_t::size);
        add_handler(collection::close_cursor, &manager_dispatcher_t::close_cursor);
        log_.trace("manager_dispatcher_t start thread pool");
        e_->start();
    }

    manager_dispatcher_t::~manager_dispatcher_t() {
        ZoneScoped;
        e_->stop();
    }

    auto manager_dispatcher_t::executor_impl() noexcept -> goblin_engineer::abstract_executor* {
        return e_.get();
    }

    //NOTE: behold thread-safety!
    auto manager_dispatcher_t::enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void {
        ZoneScoped;
        set_current_message(std::move(msg));
        execute();
    }
    auto manager_dispatcher_t::add_actor_impl(goblin_engineer::actor a) -> void {
        actor_storage_.emplace_back(std::move(a));
    }
    auto manager_dispatcher_t::add_supervisor_impl(goblin_engineer::supervisor) -> void {
        log_.error("manager_dispatcher_t::add_supervisor_impl");
    }

    dispatcher_t::dispatcher_t(goblin_engineer::supervisor_t* manager_database, goblin_engineer::address_t mdb, goblin_engineer::address_t mwal,
                               goblin_engineer::address_t mdisk, log_t& log, std::string name)
        : goblin_engineer::abstract_service(manager_database, std::move(name))
        , log_(log.clone())
        , mdb_(mdb)
        , mwal_(mwal)
        , mdisk_(mdisk) {
        log_.trace("dispatcher_t::dispatcher_t name:{}", type());
        add_handler(manager_database::create_database, &dispatcher_t::create_database);
        add_handler("create_database_finish", &dispatcher_t::create_database_finish);
        add_handler(database::create_collection, &dispatcher_t::create_collection);
        add_handler("create_collection_finish", &dispatcher_t::create_collection_finish);
        add_handler(database::drop_collection, &dispatcher_t::drop_collection);
        add_handler("drop_collection_finish_collection", &dispatcher_t::drop_collection_finish_collection);
        add_handler("drop_collection_finish", &dispatcher_t::drop_collection_finish);
        add_handler(collection::insert_one, &dispatcher_t::insert_one);
        add_handler(collection::insert_many, &dispatcher_t::insert_many);
        add_handler("insert_one_finish", &dispatcher_t::insert_one_finish);
        add_handler("insert_many_finish", &dispatcher_t::insert_many_finish);
        add_handler(collection::find, &dispatcher_t::find);
        add_handler("find_finish", &dispatcher_t::find_finish);
        add_handler(collection::find_one, &dispatcher_t::find_one);
        add_handler("find_one_finish", &dispatcher_t::find_one_finish);
        add_handler(collection::delete_one, &dispatcher_t::delete_one);
        add_handler(collection::delete_many, &dispatcher_t::delete_many);
        add_handler("delete_finish", &dispatcher_t::delete_finish);
        add_handler(collection::update_one, &dispatcher_t::update_one);
        add_handler(collection::update_many, &dispatcher_t::update_many);
        add_handler("update_finish", &dispatcher_t::update_finish);
        add_handler(collection::size, &dispatcher_t::size);
        add_handler("size_finish", &dispatcher_t::size_finish);
        add_handler(collection::close_cursor, &dispatcher_t::close_cursor);
    }

    void dispatcher_t::create_database(components::session::session_id_t& session, std::string& name, goblin_engineer::address_t address) {
        trace(log_,"dispatcher_t::create_database: session {} , name create_database {}", session.data(), name);
        session_to_address_.emplace(session, address);
        goblin_engineer::send(mdb_, dispatcher_t::address(), manager_database::create_database, session, std::move(name));
    }

    void dispatcher_t::create_database_finish(components::session::session_id_t& session, storage::database_create_result result, goblin_engineer::address_t database) {
        auto type = database.type();
        trace(log_,"dispatcher_t::create_database: session {} , name create_database {}", session.data(), type);

        if (result.created_) {
            auto md = address_book("manager_dispatcher");
            goblin_engineer::link(md, database);
            database_address_book_.emplace(type, database);
            goblin_engineer::send(mdisk_, dispatcher_t::address(), disk::route::append_database, session, std::string(type));
            log_.trace("add database_create_result");
        }
        goblin_engineer::send(session_to_address_.at(session).address(), dispatcher_t::address(), "create_database_finish", session, result);
        session_to_address_.erase(session);
    }

    void dispatcher_t::create_collection(components::session::session_id_t& session, std::string& database_name, std::string& collections_name, goblin_engineer::address_t address) {
        trace(log_,"dispatcher_t::create_collection: session {} , database_name {} , collection_name {}", session.data(), database_name, collections_name);
        session_to_address_.emplace(session, address);
        goblin_engineer::send(database_address_book_.at(database_name), dispatcher_t::address(), "create_collection", session, collections_name, mdisk_);
    }

    void dispatcher_t::create_collection_finish(components::session::session_id_t& session, storage::collection_create_result result, std::string& database_name, goblin_engineer::address_t collection) {
        auto type = collection.type();
        trace(log_,"create_collection_finish: {}", type);
        if (result.created_) {
            auto md = address_book("manager_dispatcher");
            goblin_engineer::link(md, collection);
            collection_address_book_.emplace(key_collection_t(database_name, std::string(type)), collection);
            goblin_engineer::send(mdisk_, dispatcher_t::address(), disk::route::append_collection, session, database_name, std::string(type));
            log_.trace("add database_create_result");
        }
        goblin_engineer::send(session_to_address_.at(session).address(), dispatcher_t::address(), "create_collection_finish", session, result);
        session_to_address_.erase(session);
    }
    void dispatcher_t::drop_collection(components::session::session_id_t& session, std::string& database_name, std::string& collection_name, goblin_engineer::address_t address) {
        trace(log_,"dispatcher_t::drop_collection: session {} , database_name {} , collection_name {}", session.data(), database_name, collection_name);
        auto it_collection = collection_address_book_.find({database_name, collection_name});
        if (it_collection != collection_address_book_.end()) {
            session_to_address_.emplace(session, address);
            goblin_engineer::send(it_collection->second, dispatcher_t::address(), database::drop_collection, session);
        } else {
            goblin_engineer::send(address, dispatcher_t::address(), "drop_collection_finish", session, result_drop_collection(false));
        }
    }
    void dispatcher_t::drop_collection_finish_collection(components::session::session_id_t& session, result_drop_collection& result, std::string& database_name, std::string& collection_name) {
        trace(log_,"dispatcher_t::drop_collection_finish_collection: database_name {} , collection_name {}, result: {}", session.data(), database_name, collection_name, result.is_success());
        if (result.is_success()) {
            goblin_engineer::send(database_address_book_.at(database_name), dispatcher_t::address(), database::drop_collection, session, collection_name);
        } else {
            goblin_engineer::send(session_to_address_.at(session).address(), dispatcher_t::address(), "drop_collection_finish", session, result);
            session_to_address_.erase(session);
        }
    }
    void dispatcher_t::drop_collection_finish(components::session::session_id_t& session, result_drop_collection& result, std::string& database_name, goblin_engineer::address_t collection) {
        auto type = collection.type();
        trace(log_,"drop_collection_finish: {}", type);
        if (result.is_success()) {
            //auto md = address_book("manager_dispatcher");
            //goblin_engineer::link(md,collection);
            collection_address_book_.erase({database_name, std::string(type)});
            goblin_engineer::send(mdisk_, dispatcher_t::address(), disk::route::remove_collection, session, database_name, std::string(type));
            trace(log_,"collection {} dropped", type);
        }
        goblin_engineer::send(session_to_address_.at(session).address(), dispatcher_t::address(), "drop_collection_finish", session, result);
        session_to_address_.erase(session);
    }

    void dispatcher_t::insert_one(components::session::session_id_t& session, std::string &database_name, std::string& collection, components::document::document_ptr& document, goblin_engineer::address_t address) {
        log_.debug("dispatcher_t::insert_one: session:{}, database: {}, collection: {}", session.data(), database_name, collection);
        key_collection_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            session_to_address_.emplace(session, address);
            goblin_engineer::send(it_collection->second, dispatcher_t::address(), collection::insert_one, session, std::move(document));
        } else {
            goblin_engineer::send(address, dispatcher_t::address(), "insert_one_finish", session, result_insert_one());
        }
    }

    void dispatcher_t::insert_many(components::session::session_id_t &session, std::string& database_name, std::string &collection, std::list<components::document::document_ptr> &documents, goblin_engineer::address_t address) {
        log_.debug("dispatcher_t::insert_many: session:{}, database: {}, collection: {}", session.data(), database_name, collection);
        key_collection_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            make_session(session_to_address_,session, session_t(address,std::move(insert_many_t(database_name,collection,documents))));
            goblin_engineer::send(it_collection->second, dispatcher_t::address(), collection::insert_many, session, std::move(documents));
        } else {
            goblin_engineer::send(address, dispatcher_t::address(), "insert_many_finish", session, result_insert_many());
        }
    }

    void dispatcher_t::insert_one_finish(components::session::session_id_t& session, result_insert_one& result) {
        trace(log_,"dispatcher_t::insert_one_finish session: {}", session.data());
        goblin_engineer::send(session_to_address_.at(session).address(), dispatcher_t::address(), "insert_one_finish", session, result);
        if (!result.empty()) {
            goblin_engineer::send(mdisk_, dispatcher_t::address(), disk::route::write_documents_flush, session); //todo after wal
        }
        session_to_address_.erase(session);
    }

    void dispatcher_t::insert_many_finish(components::session::session_id_t& session, result_insert_many& result) {
        trace(log_,"dispatcher_t::insert_many_finish session: {}", session.data());
        auto&s = ::find(session_to_address_,session);
        trace(log_,"dispatcher_t::insert_many_finish session: 0 ");
        goblin_engineer::send(mwal_, dispatcher_t::address(), "insert_many", s.get<insert_many_t>());
        trace(log_,"dispatcher_t::insert_many_finish session: 1 ");
        if (!result.empty()) {
            goblin_engineer::send(mdisk_, dispatcher_t::address(), disk::route::write_documents_flush, session); //todo after wal
        }
        goblin_engineer::send(s.address(), dispatcher_t::address(), "insert_many_finish", session, result);
        trace(log_,"dispatcher_t::insert_many_finish session: 2 ");
        ::remove(session_to_address_,session);
        trace(log_,"dispatcher_t::insert_many_finish session: 3 ");
    }

    void dispatcher_t::find(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition, goblin_engineer::address_t address) {
        log_.debug("dispatcher_t::find: session:{}, database: {}, collection: {}", session.data(), database_name, collection);

        key_collection_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            session_to_address_.emplace(session, address);
            goblin_engineer::send(it_collection->second, dispatcher_t::address(), collection::find, session, components::parser::parse_find_condition(condition));
        } else {
            goblin_engineer::send(address, dispatcher_t::address(), "find_finish", session, result_find());
        }
    }
    void dispatcher_t::find_finish(components::session::session_id_t& session, components::cursor::sub_cursor_t* cursor) {
        trace(log_,"dispatcher_t::find_finish session: {}", session.data());
        auto result = new components::cursor::cursor_t();
        if (cursor) {
            result->push(cursor);
        }
        goblin_engineer::send(session_to_address_.at(session).address(), dispatcher_t::address(), "find_finish", session, result);
        session_to_address_.erase(session);
    }

    void dispatcher_t::find_one(components::session::session_id_t &session, std::string& database_name, std::string &collection, components::document::document_ptr &condition, goblin_engineer::address_t address) {
        log_.debug("dispatcher_t::find_one: session:{}, database: {}, collection: {}", session.data(), database_name, collection);
        key_collection_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            session_to_address_.emplace(session, address);
            goblin_engineer::send(it_collection->second, dispatcher_t::address(), collection::find_one, session, components::parser::parse_find_condition(condition));
        } else {
            goblin_engineer::send(address, dispatcher_t::address(), "find_one_finish", session, result_find_one());
        }
    }
    void dispatcher_t::find_one_finish(components::session::session_id_t& session, result_find_one& result) {
        trace(log_,"dispatcher_t::find_one_finish session: {}", session.data());
        goblin_engineer::send(session_to_address_.at(session).address(), dispatcher_t::address(), "find_one_finish", session, result);
        session_to_address_.erase(session);
    }

    void dispatcher_t::delete_one(session_id_t &session, std::string &database_name, std::string &collection, components::document::document_ptr &condition, goblin_engineer::address_t address) {
        log_.debug("dispatcher_t::delete_one: session:{}, database: {}, collection: {}", session.data(), database_name, collection);
        key_collection_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            session_to_address_.emplace(session, address);
            goblin_engineer::send(it_collection->second, dispatcher_t::address(), collection::delete_one, session, components::parser::parse_find_condition(condition));
        } else {
            goblin_engineer::send(address, dispatcher_t::address(), "delete_finish", session, result_delete());
        }
    }

    void dispatcher_t::delete_many(session_id_t &session, std::string &database_name, std::string &collection, components::document::document_ptr &condition, goblin_engineer::address_t address) {
        log_.debug("dispatcher_t::delete_many: session:{}, database: {}, collection: {}", session.data(), database_name, collection);
        key_collection_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            session_to_address_.emplace(session, address);
            goblin_engineer::send(it_collection->second, dispatcher_t::address(), collection::delete_many, session, components::parser::parse_find_condition(condition));
        } else {
            goblin_engineer::send(address, dispatcher_t::address(), "delete_finish", session, result_delete());
        }
    }
    void dispatcher_t::delete_finish(session_id_t& session, result_delete& result) {
        trace(log_,"dispatcher_t::delete_finish session: {}", session.data());
        goblin_engineer::send(session_to_address_.at(session).address(), dispatcher_t::address(), "delete_finish", session, result);
        if (!result.empty()) {
            goblin_engineer::send(mdisk_, dispatcher_t::address(), disk::route::remove_documents_flush, session);
        }
        session_to_address_.erase(session);
    }

    void dispatcher_t::update_one(session_id_t &session, std::string &database_name, std::string &collection, components::document::document_ptr &condition, components::document::document_ptr &update, bool upsert, goblin_engineer::address_t address) {
        log_.debug("dispatcher_t::update_one: session:{}, database: {}, collection: {}", session.data(), database_name, collection);
        key_collection_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            session_to_address_.emplace(session, address);
            goblin_engineer::send(it_collection->second, dispatcher_t::address(), collection::update_one, session, components::parser::parse_find_condition(condition), std::move(update), upsert);
        } else {
            goblin_engineer::send(address, dispatcher_t::address(), "update_finish", session, result_update());
        }
    }

    void dispatcher_t::update_many(session_id_t &session, std::string &database_name, std::string &collection, components::document::document_ptr &condition, components::document::document_ptr &update, bool upsert, goblin_engineer::address_t address) {
        log_.debug("dispatcher_t::update_many: session:{}, database: {}, collection: {}", session.data(), database_name, collection);
        key_collection_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            session_to_address_.emplace(session, address);
            goblin_engineer::send(it_collection->second, dispatcher_t::address(), collection::update_many, session, components::parser::parse_find_condition(condition), std::move(update), upsert);
        } else {
            goblin_engineer::send(address, dispatcher_t::address(), "update_finish", session, result_update());
        }
    }
    void dispatcher_t::update_finish(session_id_t& session, result_update& result) {
        trace(log_,"dispatcher_t::update_finish session: {}", session.data());
        goblin_engineer::send(session_to_address_.at(session).address(), dispatcher_t::address(), "update_finish", session, result);
        if (!result.empty()) {
            goblin_engineer::send(mdisk_, dispatcher_t::address(), disk::route::write_documents_flush, session); //todo after wal
        }
        session_to_address_.erase(session);
    }
    void dispatcher_t::size(components::session::session_id_t& session, std::string& database_name, std::string& collection, goblin_engineer::address_t address) {
        trace(log_,"dispatcher_t::size: session:{}, database: {}, collection: {}", session.data(), database_name, collection);
        key_collection_t key(database_name, collection);
        auto it_collection = collection_address_book_.find(key);
        if (it_collection != collection_address_book_.end()) {
            session_to_address_.emplace(session, address);
            goblin_engineer::send(it_collection->second, dispatcher_t::address(), collection::size, session);
        } else {
            goblin_engineer::send(address, dispatcher_t::address(), "size_finish", session, result_size());
        }
    }
    void dispatcher_t::size_finish(components::session::session_id_t& session, result_size& result) {
        trace(log_,"dispatcher_t::size_finish session: {}", session.data());
        goblin_engineer::send(session_to_address_.at(session).address(), dispatcher_t::address(), "size_finish", session, result);
        session_to_address_.erase(session);
    }
    void dispatcher_t::close_cursor(components::session::session_id_t& session) {
        trace(log_," dispatcher_t::close_cursor ");
        trace(log_,"Session : {}", session.data());
        auto it = cursor_.find(session);
        if (it != cursor_.end()) {
            for (auto& i : *it->second) {
                goblin_engineer::send(i->address(), address(), "close_cursor", session);
            }
            cursor_.erase(it);
        } else {
            log_.error("Not find session : {}", session.data());
        }
    }

    void manager_dispatcher_t::create(components::session::session_id_t& session, std::string& name) {
        trace(log_,"manager_dispatcher_t::create session: {} , name: {} ", session.data(), name);
        auto address = spawn_actor<dispatcher_t>(address_book("manager_database"), address_book("manager_wal"), address_book("manager_disk"), log_, std::move(name));
        std::string type(address.type().data(), address.type().size());
        trace(log_,"address: {} pointer: {}", type, address.get());
        dispatcher_to_address_book_.emplace(type, address);
        dispathers_.emplace_back(address);
    }
    void manager_dispatcher_t::connect_me(components::session::session_id_t& session, std::string& name) {
        trace(log_,"manager_dispatcher_t::connect_me session: {} , name: {} ", session.data(), name);
        auto dispatcher = dispatcher_to_address_book_.at(name);
        trace(log_,"dispatcher: {}", dispatcher.type());
        goblin_engineer::link(dispatcher, current_message()->sender());
    }

    void manager_dispatcher_t::create_database(session_id_t& session, std::string& name) {
        trace(log_,"manager_dispatcher_t::create_database session: {} , name: {} ", session.data(), name);
        return goblin_engineer::send(dispathers_[0], address(), manager_database::create_database, session, std::move(name), current_message()->sender());
    }

    void manager_dispatcher_t::create_collection(session_id_t& session, std::string& database_name, std::string& collection_name) {
        log_.trace("manager_dispatcher_t::create_collection session: {} , database name: {} , collection name: {} ", session.data(), database_name, collection_name);
        return goblin_engineer::send(dispathers_[0], address(), database::create_collection, session, std::move(database_name), std::move(collection_name), current_message()->sender());
    }

    void manager_dispatcher_t::drop_collection(components::session::session_id_t& session, std::string& database_name, std::string& collection_name) {
        log_.trace("manager_dispatcher_t::drop_collection session: {} , database name: {} , collection name: {} ", session.data(), database_name, collection_name);
        return goblin_engineer::send(dispathers_[0], address(), database::drop_collection, session, std::move(database_name), std::move(collection_name), current_message()->sender());
    }


    void manager_dispatcher_t::insert_one(session_id_t& session, std::string& database_name, std::string& collection_name, components::document::document_ptr& document) {
        log_.trace("manager_dispatcher_t::insert_one session: {}, database: {}, collection name: {} ", session.data(), database_name, collection_name);
        return goblin_engineer::send(dispathers_[0], address(), collection::insert_one, session, std::move(database_name), std::move(collection_name), std::move(document), current_message()->sender());
    }

    void manager_dispatcher_t::insert_many(session_id_t& session, std::string& database_name, std::string& collection_name, std::list<components::document::document_ptr>& documents) {
        log_.trace("manager_dispatcher_t::insert_many session: {}, database: {}, collection name: {} ", session.data(), database_name, collection_name);
        return goblin_engineer::send(dispathers_[0],address(), collection::insert_many, session, std::move(database_name), std::move(collection_name), std::move(documents), current_message()->sender());
    }

    void manager_dispatcher_t::find(session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition) {
        log_.trace("manager_dispatcher_t::find session: {}, database: {}, collection name: {} ", session.data(), database_name, collection);
        return goblin_engineer::send(dispathers_[0], address(), collection::find, session, std::move(database_name), std::move(collection), std::move(condition), current_message()->sender());
    }

    void manager_dispatcher_t::find_one(session_id_t &session, std::string& database_name, std::string &collection, components::document::document_ptr &condition) {
        log_.trace("manager_dispatcher_t::find_one session: {}, database: {}, collection name: {} ", session.data(), database_name, collection);
        return goblin_engineer::send(dispathers_[0], address(), collection::find_one, session, std::move(database_name), std::move(collection), std::move(condition), current_message()->sender());
    }

    void manager_dispatcher_t::delete_one(session_id_t &session, std::string &database_name, std::string &collection, components::document::document_ptr &condition) {
        log_.trace("manager_dispatcher_t::delete_one session: {}, database: {}, collection name: {} ", session.data(), database_name, collection);
        return goblin_engineer::send(dispathers_[0], address(), collection::delete_one, session, std::move(database_name), std::move(collection), std::move(condition), current_message()->sender());
    }

    void manager_dispatcher_t::delete_many(session_id_t &session, std::string &database_name, std::string &collection, components::document::document_ptr &condition) {
        log_.trace("manager_dispatcher_t::delete_many session: {}, database: {}, collection name: {} ", session.data(), database_name, collection);
        return goblin_engineer::send(dispathers_[0], address(), collection::delete_many, session, std::move(database_name), std::move(collection), std::move(condition), current_message()->sender());
    }

    void manager_dispatcher_t::update_one(session_id_t &session, std::string &database_name, std::string &collection, components::document::document_ptr &condition, components::document::document_ptr &update, bool upsert) {
        log_.trace("manager_dispatcher_t::update_one session: {}, database: {}, collection name: {} ", session.data(), database_name, collection);
        return goblin_engineer::send(dispathers_[0], address(), collection::update_one, session, std::move(database_name), std::move(collection), std::move(condition), std::move(update), upsert, current_message()->sender());
    }

    void manager_dispatcher_t::update_many(session_id_t &session, std::string &database_name, std::string &collection, components::document::document_ptr &condition, components::document::document_ptr &update, bool upsert) {
        log_.trace("manager_dispatcher_t::update_many session: {}, database: {}, collection name: {} ", session.data(), database_name, collection);
        return goblin_engineer::send(dispathers_[0], address(), collection::update_many, session, std::move(database_name), std::move(collection), std::move(condition), std::move(update), upsert, current_message()->sender());
    }

    void manager_dispatcher_t::size(session_id_t& session, std::string& database_name, std::string& collection) {
        trace(log_,"manager_dispatcher_t::size session: {} , database: {}, collection name: {} ", session.data(), database_name, collection);
        goblin_engineer::send(dispathers_[0], address(), collection::size, session, std::move(database_name), std::move(collection), current_message()->sender());
    }
    void manager_dispatcher_t::close_cursor(session_id_t& session) {
    }

    key_collection_t::key_collection_t(const std::string& database, const std::string& collection)
        : database_(database)
        , collection_(collection) {
    }

    const std::string& key_collection_t::database() const {
        return database_;
    }

    const std::string& key_collection_t::collection() const {
        return collection_;
    }

    bool key_collection_t::operator==(const key_collection_t& other) const {
        return database_ == other.database_ && collection_ == other.collection_;
    }

    std::size_t key_collection_t::hash::operator()(const key_collection_t& key) const {
        return std::hash<std::string>()(key.database()) ^ std::hash<std::string>()(key.collection());
    }

} // namespace services::dispatcher