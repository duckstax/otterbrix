#include "dispatcher.hpp"
#include "components/tracy/tracy.hpp"
#include <services/storage/route.hpp>
#include <components/document/document.hpp>
#include "route.hpp"
#include <services/storage/database.hpp>

namespace services::dispatcher {

    manager_dispatcher_t::manager_dispatcher_t(log_t& log, size_t num_workers, size_t max_throughput)
        : manager("manager_dispatcher")
        , log_(log.clone())
        , e_(new goblin_engineer::shared_work(num_workers, max_throughput), goblin_engineer::detail::thread_pool_deleter()) {
        ZoneScoped;
        log_.trace("manager_dispatcher_t::manager_dispatcher_t num_workers : {} , max_throughput: {}",num_workers,max_throughput);
        add_handler("create",&manager_dispatcher_t::create);
        add_handler("connect_me",&manager_dispatcher_t::connect_me);
        add_handler(manager_database::create_database, &manager_dispatcher_t::create_database);
        add_handler(database::create_collection, &manager_dispatcher_t::create_collection);
        add_handler(collection::insert_one, &manager_dispatcher_t::insert_one);
        add_handler(collection::insert_many, &manager_dispatcher_t::insert_many);
        add_handler(collection::find, &manager_dispatcher_t::find);
        add_handler(collection::find_one, &manager_dispatcher_t::find_one);
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

    dispatcher_t::dispatcher_t(goblin_engineer::supervisor_t* manager_database,goblin_engineer::address_t mdb , log_t& log,std::string name)
        : goblin_engineer::abstract_service(manager_database, std::move(name))
        , log_(log.clone())
        , mdb_(mdb) {
        log_.trace("dispatcher_t::dispatcher_t name:{}",type());
        add_handler(manager_database::create_database, &dispatcher_t::create_database);
        add_handler("create_database_finish", &dispatcher_t::create_database_finish);
        add_handler(database::create_collection, &dispatcher_t::create_collection);
        add_handler("create_collection_finish", &dispatcher_t::create_collection_finish);
        add_handler(collection::insert_one, &dispatcher_t::insert_one);
        add_handler(collection::insert_many, &dispatcher_t::insert_many);
        add_handler("insert_one_finish", &dispatcher_t::insert_one_finish);
        add_handler("insert_many_finish", &dispatcher_t::insert_many_finish);
        add_handler(collection::find, &dispatcher_t::find);
        add_handler("find_finish", &dispatcher_t::find_finish);
        add_handler(collection::find_one, &dispatcher_t::find_one);
        add_handler("find_one_finish", &dispatcher_t::find_one_finish);
        add_handler(collection::size, &dispatcher_t::size);
        add_handler("size_finish", &dispatcher_t::size_finish);
        add_handler(collection::close_cursor, &dispatcher_t::close_cursor);
    }

    void dispatcher_t::create_database(components::session::session_t& session, std::string& name,goblin_engineer::address_t address) {
        log_.debug("dispatcher_t::create_database: session {} , name create_database {}",session.data(), name);
        session_to_address_.emplace(session, address);
        goblin_engineer::send(mdb_,dispatcher_t::address() , manager_database::create_database, session, std::move(name));
    }

    void dispatcher_t::create_database_finish(components::session::session_t& session, storage::database_create_result result, goblin_engineer::address_t database) {
        auto type = database.type();
        log_.debug("dispatcher_t::create_database: session {} , name create_database {}",session.data(), type);

        if (result.created_) {
            auto md = address_book("manager_dispatcher");
            goblin_engineer::link(md,database);
            database_address_book_.emplace(type, database);
            log_.trace("add database_create_result");
        }
        goblin_engineer::send(session_to_address_.at(session), dispatcher_t::address(), "create_database_finish", session,result);
        session_to_address_.erase(session);
    }

    void dispatcher_t::create_collection(components::session::session_t& session, std::string& database_name,std::string& collections_name,goblin_engineer::address_t address) {
        log_.debug("dispatcher_t::create_collection: session {} , database_name {} , collections_name",session.data(), database_name, collections_name);
        session_to_address_.emplace(session,address);
        goblin_engineer::send(database_address_book_.at(database_name), dispatcher_t::address(), "create_collection", session, collections_name);
    }

    void dispatcher_t::create_collection_finish(components::session::session_t& session, storage::collection_create_result result ,goblin_engineer::address_t collection) {
        auto type = collection.type();
        log_.debug("create_collection_finish: {}", type);
        if(result.created_){
            auto md = address_book("manager_dispatcher");
            goblin_engineer::link(md,collection);
            collection_address_book_.emplace(type,collection);
            log_.trace("add database_create_result");
        }
        goblin_engineer::send(session_to_address_.at(session),dispatcher_t::address(),"create_collection_finish",session,result);
        session_to_address_.erase(session);
    }
    void dispatcher_t::insert_one(components::session::session_t& session, std::string& collection, components::document::document_t& document, goblin_engineer::address_t address) {
        log_.debug("dispatcher_t::insert_one: session:{}, collection: {}", session.data(), collection);
        session_to_address_.emplace(session, address);
        goblin_engineer::send(collection_address_book_.at(collection), dispatcher_t::address(), collection::insert_one, session, collection, std::move(document));
    }
    void dispatcher_t::insert_many(components::session::session_t &session, std::string &collection, std::list<components::document::document_t> &documents, goblin_engineer::address_t address) {
        log_.debug("dispatcher_t::insert_many: session:{}, collection: {}", session.data(), collection);
        session_to_address_.emplace(session, address);
        goblin_engineer::send(collection_address_book_.at(collection), dispatcher_t::address(), collection::insert_many, session, collection, std::move(documents));
    }
    void dispatcher_t::insert_one_finish(components::session::session_t& session, result_insert_one& result) {
        log_.debug("dispatcher_t::insert_one_finish session: {}",session.data());
        goblin_engineer::send(session_to_address_.at(session),dispatcher_t::address(),"insert_one_finish",session,result);
        session_to_address_.erase(session);
    }
    void dispatcher_t::insert_many_finish(components::session::session_t &session, result_insert_many &result) {
        log_.debug("dispatcher_t::insert_many_finish session: {}",session.data());
        goblin_engineer::send(session_to_address_.at(session),dispatcher_t::address(),"insert_many_finish",session,result);
        session_to_address_.erase(session);
    }
    void dispatcher_t::find(components::session::session_t& session, std::string& collection, components::document::document_t& condition, goblin_engineer::address_t address) {
        log_.debug("dispatcher_t::find: session:{}, collection: {}", session.data(), collection);
        session_to_address_.emplace(session, address);
        goblin_engineer::send(collection_address_book_.at(collection), dispatcher_t::address(), collection::find, session, collection, std::move(condition));
    }
    void dispatcher_t::find_finish(components::session::session_t& session, components::cursor::sub_cursor_t* cursor) {
        log_.debug("dispatcher_t::find_finish session: {}", session.data());
        auto result = new components::cursor::cursor_t();
        result->push(cursor);
        goblin_engineer::send(session_to_address_.at(session), dispatcher_t::address(), "find_finish", session, result);
        session_to_address_.erase(session);
    }
    void dispatcher_t::find_one(components::session::session_t &session, std::string &collection, components::document::document_t &condition, goblin_engineer::address_t address) {
        log_.debug("dispatcher_t::find_one: session:{}, collection: {}", session.data(), collection);
        session_to_address_.emplace(session, address);
        goblin_engineer::send(collection_address_book_.at(collection), dispatcher_t::address(), collection::find_one, session, collection, std::move(condition));
    }
    void dispatcher_t::find_one_finish(components::session::session_t &session, result_find_one &result) {
        log_.debug("dispatcher_t::find_one_finish session: {}", session.data());
        goblin_engineer::send(session_to_address_.at(session), dispatcher_t::address(), "find_one_finish", session, result);
        session_to_address_.erase(session);
    }
    void dispatcher_t::size(components::session::session_t& session, std::string& collection, goblin_engineer::address_t address) {
        log_.debug("dispatcher_t::size: session:{} , collection: {}", session.data(), collection);
        session_to_address_.emplace(session, address);
        goblin_engineer::send(collection_address_book_.at(collection), dispatcher_t::address(), collection::size, session, collection);
    }
    void dispatcher_t::size_finish(components::session::session_t& session, result_size& result) {
        log_.debug("dispatcher_t::size_finish session: {}", session.data());
        goblin_engineer::send(session_to_address_.at(session), dispatcher_t::address(), "size_finish", session, result);
        session_to_address_.erase(session);
    }
    void dispatcher_t::close_cursor(components::session::session_t& session) {
        log_.debug(" dispatcher_t::close_cursor ");
        log_.debug("Session : {}", session.data());
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

    void manager_dispatcher_t::create(components::session::session_t& session, std::string& name) {
        log_.trace("manager_dispatcher_t::create session: {} , name: {} ",session.data(),name);
        auto address =  spawn_actor<dispatcher_t>(address_book("manager_database"),log_,std::move(name));
        std::string type(address.type().data(),address.type().size());
        log_.trace("address: {} pointer: {}",type,address.get());
        dispatcher_to_address_book_.emplace(type,address);
        dispathers_.emplace_back(address);
    }
    void manager_dispatcher_t::connect_me(components::session::session_t& session, std::string& name) {
        log_.trace("manager_dispatcher_t::connect_me session: {} , name: {} ",session.data(),name);
        auto dispatcher = dispatcher_to_address_book_.at(name);
        log_.trace("dispatcher: {}",dispatcher.type());
        goblin_engineer::link(dispatcher,current_message()->sender());
    }

    void manager_dispatcher_t::create_database(session_t& session, std::string& name) {
        log_.trace("manager_dispatcher_t::create_database session: {} , name: {} ",session.data(),name);
        return goblin_engineer::send(dispathers_[0],address(),manager_database::create_database,session,std::move(name),current_message()->sender());
    }

    void manager_dispatcher_t::create_collection(session_t& session, std::string& database_name,std::string& collection_name) {
        log_.trace("manager_dispatcher_t::create_collection session: {} , database name: {} , collection name: {} ",session.data(),database_name,collection_name);
        return goblin_engineer::send(dispathers_[0],address(),database::create_collection,session,std::move(database_name),std::move(collection_name),current_message()->sender());
    }

    void manager_dispatcher_t::insert_one(session_t& session, std::string& collection_name, components::document::document_t& document) {
        log_.trace("manager_dispatcher_t::insert_one session: {}, collection name: {} ", session.data(), collection_name);
        return goblin_engineer::send(dispathers_[0], address(), collection::insert_one, session, std::move(collection_name), std::move(document), current_message()->sender());
    }

    void manager_dispatcher_t::insert_many(session_t& session, std::string& collection_name, std::list<components::document::document_t>& documents) {
        log_.trace("manager_dispatcher_t::insert_many session: {}, collection name: {} ", session.data(), collection_name);
        return goblin_engineer::send(dispathers_[0],address(), collection::insert_many, session,std::move(collection_name), std::move(documents), current_message()->sender());
    }

    void manager_dispatcher_t::find(session_t& session, std::string& collection, components::document::document_t& condition) {
        log_.trace("manager_dispatcher_t::find session: {}, collection name: {} ", session.data(), collection);
        return goblin_engineer::send(dispathers_[0], address(), collection::find, session, std::move(collection), std::move(condition), current_message()->sender());
    }

    void manager_dispatcher_t::find_one(components::session::session_t &session, std::string &collection, components::document::document_t &condition) {
        log_.trace("manager_dispatcher_t::find_one session: {}, collection name: {} ", session.data(), collection);
        return goblin_engineer::send(dispathers_[0], address(), collection::find_one, session, std::move(collection), std::move(condition), current_message()->sender());
    }

    void manager_dispatcher_t::size(session_t& session, std::string& collection) {
        log_.trace("manager_dispatcher_t::size session: {} , collection name : {} ", session.data(), collection);
        goblin_engineer::send(dispathers_[0], address(), collection::size, session, std::move(collection), current_message()->sender());
    }
    void manager_dispatcher_t::close_cursor(session_t& session) {
    }
}