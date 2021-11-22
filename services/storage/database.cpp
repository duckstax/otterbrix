#include "database.hpp"

#include "collection.hpp"

#include "tracy/tracy.hpp"
#include "result_database.hpp"

namespace services::storage {

    manager_database_t::manager_database_t(log_t& log, size_t num_workers, size_t max_throughput)
        : goblin_engineer::abstract_manager_service("manager_database")
        , log_(log.clone())
        , e_(new goblin_engineer::shared_work(num_workers, max_throughput), goblin_engineer::detail::thread_pool_deleter()) {
        ZoneScoped;
        add_handler(manager_database::create_database, &manager_database_t::create);
        log_.debug("manager_database_t start thread pool");
        e_->start();
    }

    manager_database_t::~manager_database_t() {
        ZoneScoped;
        e_->stop();
    }

    auto manager_database_t::executor_impl() noexcept -> goblin_engineer::abstract_executor* {
        return e_.get();
    }

    //NOTE: behold thread-safety!
    auto manager_database_t::enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void {
        ZoneScoped;
        set_current_message(std::move(msg));
        execute();
    }

    void manager_database_t::create(session_t& session, std::string& name) {
        log_.debug("manager_database_t:create {}", name);
        auto address = spawn_supervisor<database_t>(std::string(name),log_,1,1000);
        databases_.emplace(address.type(),address);
        auto self  = manager_database_t::address();
        return goblin_engineer::send(current_message()->sender(),self,"create_database_finish",session,database_create_result(true),address);
    }

    database_t::database_t(goblin_engineer::supervisor_t* supervisor, std::string name, log_t& log, size_t num_workers, size_t max_throughput)
        : goblin_engineer::abstract_manager_service(supervisor,std::move(name))
        , log_(log.clone())
        , e_(new goblin_engineer::shared_work(num_workers, max_throughput), goblin_engineer::detail::thread_pool_deleter()) {
        ZoneScoped;
        add_handler(database::create_collection, &database_t::create);
        add_handler(database::drop_collection, &database_t::drop);
        e_->start();
    }

    database_t::~database_t() {
        ZoneScoped;
        e_->stop();
    }

    auto database_t::executor_impl() noexcept -> goblin_engineer::abstract_executor* {
        return e_.get();
    }

    //NOTE: behold thread-safety!
    auto database_t::enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void {
        ZoneScoped;
        set_current_message(std::move(msg));
        execute();
    }

    void database_t::create(session_t& session, std::string& name) {
        log_.debug("database_t::create {}", name);
        auto address = spawn_actor<collection_t>(std::string(name),log_);
        collections_.emplace(address.type(),address);
        auto self  = database_t::address();
        return goblin_engineer::send(current_message()->sender(),self,"create_collection_finish",session,collection_create_result(true),std::string(self.type()),address);
    }

    void database_t::drop(components::session::session_t &session, std::string &name) {
        log_.debug("database_t::drop {}", name);
        auto self = database_t::address();
        auto collection = collections_.find(name);
        if (collection != collections_.end()) {
            auto address = collection->second;
            collections_.erase(collection);
            return goblin_engineer::send(current_message()->sender(),self,"drop_collection_finish",session,result_drop_collection(true),std::string(self.type()),address);
        }
        return goblin_engineer::send(current_message()->sender(),self,"drop_collection_finish",session,result_drop_collection(false),std::string(self.type()),self);
    }

} // namespace kv