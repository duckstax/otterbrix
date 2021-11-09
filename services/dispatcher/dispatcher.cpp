#include "dispatcher.hpp"
#include "tracy/tracy.hpp"
#include <RocketJoe/services/storage/route.hpp>
#include <RocketJoe/components/document/document.hpp>
#include "route.hpp"
#include "storage/database.hpp"
#include "storage/route.hpp"

namespace services::dispatcher {

    manager_dispatcher_t::manager_dispatcher_t(log_t& log, size_t num_workers, size_t max_throughput)
        : manager("manager_dispatcher")
        , log_(log.clone())
        , e_(new goblin_engineer::shared_work(num_workers, max_throughput), goblin_engineer::detail::thread_pool_deleter()) {
        ZoneScoped;
        log_.debug("manager_dispatcher_t start thread pool");
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

    dispatcher_t::dispatcher_t(goblin_engineer::supervisor_t* manager_database, log_t& log,std::string name)
        : goblin_engineer::abstract_service(manager_database, std::move(name))
        , log_(log.clone()) {
        add_handler(manager_database::create_database, &dispatcher_t::create_database);
        add_handler("create_database_finish", &dispatcher_t::create_database_finish);
        add_handler(database::create_collection, &dispatcher_t::create_collection);
        add_handler("create_collection_finish", &dispatcher_t::create_collection_finish);
        add_handler(collection::insert, &dispatcher_t::insert);
        add_handler("insert_finish", &dispatcher_t::insert_finish);
        add_handler(collection::find, &dispatcher_t::find);
        add_handler("find_finish", &dispatcher_t::find_finish);
        add_handler(collection::size, &dispatcher_t::size);
        add_handler("size_finish", &dispatcher_t::size_finish);
        add_handler(collection::close_cursor, &dispatcher_t::close_cursor);
    }
    void dispatcher_t::create_database(components::session::session_t& session, std::string& name) {
        log_.debug("create_database_init: {}", name);
        ///create_database_and_collection_callback_ = std::move(callback);
        goblin_engineer::send(address_book("manager_database"), address(), database::create_collection, session, std::move(name));
    }
    void dispatcher_t::create_database_finish(components::session::session_t& session,storage::database_create_result result,goblin_engineer::address_t address) {
        auto type = address.type();
        log_.debug("create_database_finish: {}", type);
        if(result.created_){
            database_address_book_.emplace(type,address);
        }

    }
    void dispatcher_t::create_collection(components::session::session_t& session, std::string& name) {
        log_.debug("create_collection: {}", name);
        ///create_database_and_collection_callback_ = std::move(callback);
        goblin_engineer::send(address_book("manager_database"), address(), "create_database", session, name);
    }
    void dispatcher_t::create_collection_finish(components::session::session_t& session, storage::collection_create_result result ,goblin_engineer::address_t address) {
        auto type = address.type();
        log_.debug("create_collection_finish: {}", type);
        if(result.created_){
            collection_address_book_.emplace(type,address);
        }
    }
    void dispatcher_t::insert(components::session::session_t& session, std::string& collection, components::storage::document_t& document) {
        log_.debug("dispatcher_t::insert: {}", collection);
        ///insert_callback_ = std::move(callback);
        goblin_engineer::send(address_book("collection"), address(), "insert", session, collection, std::move(document));
    }
    void dispatcher_t::insert_finish(components::session::session_t& session, result_insert_one& result) {
        log_.debug("dispatcher_t::insert_finish");
        ///insert_callback_(result);
    }
    void dispatcher_t::find(components::session::session_t& session, std::string& collection, components::storage::document_t& condition) {
        log_.debug("dispatcher_t::find: {}", collection);
        log_.debug("Session : {}", session.data());
        ///find_callback_ = std::move(callback);
        goblin_engineer::send(address_book("collection"), address(), "find", session, collection, std::move(condition));
    }
    void dispatcher_t::find_finish(components::session::session_t& session, components::cursor::sub_cursor_t* cursor) {
        log_.debug("dispatcher_t::find_finish");
        log_.debug("Session : {}", session.data());
        auto cursor_ptr = std::make_unique<components::cursor::cursor_t>();
        cursor_ptr->push(cursor);
        auto result = cursor_.emplace(session, std::move(cursor_ptr));
        ///find_callback_(session , result.first->second.get());
    }
    void dispatcher_t::size(components::session::session_t& session, std::string& collection) {
        log_.debug("dispatcher_t::size: {}", collection);
        goblin_engineer::send(address_book("collection"), address(), "size", session, collection);
    }
    void dispatcher_t::size_finish(components::session::session_t&, result_size& result) {
        log_.debug("dispatcher_t::size_finish");
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
        spawn_actor<dispatcher_t>(log_,std::move(name));
    }
}