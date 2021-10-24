#include "dispatcher.hpp"
#include "tracy/tracy.hpp"
#include <services/storage/route.hpp>
#include <components/document/document.hpp>
#include "route.hpp"

namespace dispatcher = services::storage::dispatcher;

manager_dispatcher_t::manager_dispatcher_t(log_t& log, size_t num_workers, size_t max_throughput)
    : goblin_engineer::abstract_manager_service("manager_dispatcher")
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

auto manager_dispatcher_t::executor() noexcept -> goblin_engineer::abstract_executor* {
    return e_.get();
}
auto manager_dispatcher_t::get_executor() noexcept -> goblin_engineer::abstract_executor* {
    return e_.get();
}

//NOTE: behold thread-safety!
auto manager_dispatcher_t::enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void {
    ZoneScoped;
    set_current_message(std::move(msg));
    execute(*this);
}

dispatcher_t::dispatcher_t(manager_dispatcher_ptr manager_database, log_t& log)
    : goblin_engineer::abstract_service(manager_database, "dispatcher")
    , log_(log.clone()) {
    add_handler(duck_charmer::manager_database::create_database, &dispatcher_t::create_database);
    add_handler("create_database_finish", &dispatcher_t::create_database_finish);
    add_handler(duck_charmer::database::create_collection, &dispatcher_t::create_collection);
    add_handler("create_collection_finish", &dispatcher_t::create_collection_finish);
    add_handler(duck_charmer::collection::insert, &dispatcher_t::insert);
    add_handler("insert_finish", &dispatcher_t::insert_finish);
    add_handler(duck_charmer::collection::find, &dispatcher_t::find);
    add_handler("find_finish", &dispatcher_t::find_finish);
    add_handler(duck_charmer::collection::size, &dispatcher_t::size);
    add_handler("size_finish", &dispatcher_t::size_finish);
    add_handler(duck_charmer::collection::close_cursor, &dispatcher_t::close_cursor);
}
void dispatcher_t::create_database(duck_charmer::session_t& session, std::string& name, std::function<void(goblin_engineer::actor_address)>& callback) {
    log_.debug("create_database_init: {}", name);
    create_database_and_collection_callback_ = std::move(callback);
    goblin_engineer::send(addresses("manager_database"), self(), "create_database", session, name);
}
void dispatcher_t::create_database_finish(duck_charmer::session_t& session, goblin_engineer::actor_address address) {
    log_.debug("create_database_finish: {}", address->type());
    create_database_and_collection_callback_(address);
}
void dispatcher_t::create_collection(duck_charmer::session_t& session, std::string& name, std::function<void(goblin_engineer::actor_address)>& callback) {
    log_.debug("create_collection: {}", name);
    create_database_and_collection_callback_ = std::move(callback);
    goblin_engineer::send(addresses("manager_database"), self(), "create_database", session, name);
}
void dispatcher_t::create_collection_finish(duck_charmer::session_t& session, goblin_engineer::actor_address address) {
    log_.debug("create_collection_finish: {}", address->type());
    create_database_and_collection_callback_(address);
}
void dispatcher_t::insert(duck_charmer::session_t& session, std::string& collection,components::storage::document_t& document, std::function<void(result_insert_one&)>& callback) {
    log_.debug("dispatcher_t::insert: {}", collection);
    insert_callback_ = std::move(callback);
    goblin_engineer::send(addresses("collection"), self(), "insert", session,collection,std::move(document) );
}
void dispatcher_t::insert_finish(duck_charmer::session_t& session, result_insert_one& result) {
    log_.debug("dispatcher_t::insert_finish");
    insert_callback_(result);
}
void dispatcher_t::find(duck_charmer::session_t &session, std::string &collection, components::storage::document_t &condition, std::function<void (duck_charmer::session_t& session,components::cursor::cursor_t*)> &callback) {
    log_.debug("dispatcher_t::find: {}", collection);
    log_.debug("Session : {}" , session.data());
    log_.debug("{}",collection);
    find_callback_ = std::move(callback);
    goblin_engineer::send(addresses("collection"), self(), "find", session, collection, std::move(condition));
}
void dispatcher_t::find_finish(duck_charmer::session_t & session, components::cursor::sub_cursor_t *cursor) {
    log_.debug("dispatcher_t::find_finish");
    log_.debug("Session : {}" , session.data());
    auto cursor_ptr = std::make_unique<components::cursor::cursor_t>();
    cursor_ptr->push(cursor);
    auto result = cursor_.emplace(session,std::move(cursor_ptr));
    find_callback_(session , result.first->second.get());
}
void dispatcher_t::size(duck_charmer::session_t &session, std::string &collection, std::function<void (result_size &)> &callback) {
    log_.debug("dispatcher_t::size: {}", collection);
    size_callback_ = std::move(callback);
    goblin_engineer::send(addresses("collection"), self(), "size", session, collection);
}
void dispatcher_t::size_finish(duck_charmer::session_t &, result_size &result) {
    log_.debug("dispatcher_t::size_finish");
    size_callback_(result);
}
void dispatcher_t::close_cursor(duck_charmer::session_t& session) {
    log_.debug(" dispatcher_t::close_cursor ");
    log_.debug("Session : {}" , session.data());
   /// auto result =  cursor_.at(session);
   /// result->
}
