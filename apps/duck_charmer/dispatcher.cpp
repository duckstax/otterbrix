#include "dispatcher.hpp"
#include "tracy/tracy.hpp"
#include <services/storage/route.hpp>
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
    add_handler("create_database", &dispatcher_t::create_database);
    add_handler("create_database_finish", &dispatcher_t::create_database_finish);
    add_handler("create_collection", &dispatcher_t::create_collection);
    add_handler("create_collection_finish", &dispatcher_t::create_collection_finish);
}
void dispatcher_t::create_database(session_t& session, std::string& name, std::function<void(goblin_engineer::actor_address)>& callback) {
    log_.debug("create_database_init: {}", name);
    create_database_and_collection_callback_ = std::move(callback);
    goblin_engineer::send(addresses("manager_database"), self(), "create_database", session, name);
}
void dispatcher_t::create_database_finish(session_t& session, goblin_engineer::actor_address address) {
    log_.debug("create_database_finish: {}", address->type());
    create_database_and_collection_callback_(address);
}
void dispatcher_t::create_collection(session_t& session, std::string& name, std::function<void(goblin_engineer::actor_address)>& callback) {
    log_.debug("create_collection: {}", name);
    create_database_and_collection_callback_ = std::move(callback);
    goblin_engineer::send(addresses("manager_database"), self(), "create_database", session, name);
}
void dispatcher_t::create_collection_finish(session_t& session, goblin_engineer::actor_address address) {
    log_.debug("create_collection_finish: {}", address->type());
    create_database_and_collection_callback_(address);
}