#include "database.hpp"

#include <core/tracy/tracy.hpp>
#include <core/system_command.hpp>

#include <services/database/result_database.hpp>
#include <services/collection/collection.hpp>

#include "route.hpp"

using namespace services::database;

namespace services::database {

    manager_database_t::manager_database_t(actor_zeta::detail::pmr::memory_resource* mr,actor_zeta::scheduler_raw scheduler , log_t& log)
        : actor_zeta::cooperative_supervisor<manager_database_t>(mr, "manager_database")
        , log_(log.clone())
        , e_(scheduler) {
        ZoneScoped;
        add_handler(handler_id(route::create_databases), &manager_database_t::create_databases);
        add_handler(handler_id(route::create_database), &manager_database_t::create);
        add_handler(core::handler_id(core::route::sync), &manager_database_t::sync);
        debug(log_, "manager_database_t start thread pool");
    }

    manager_database_t::~manager_database_t() {
        ZoneScoped;
    }

    auto manager_database_t::scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* {
        return e_;
    }

    //NOTE: behold thread-safety!
    auto manager_database_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        ZoneScoped;
        std::unique_lock<spin_lock> _(lock_);
        set_current_message(std::move(msg));
        execute(this, current_message());
    }

    void manager_database_t::create_databases(session_id_t& session, std::vector<database_name_t>& databases) {
        trace(log_, "manager_database_t:create_databases, session: {}, count: {}", session.data(), databases.size());
        std::vector<actor_zeta::address_t> addresses;
        for (const auto &database_name : databases) {
            spawn_supervisor<database_t>(
                [this, &database_name, &addresses](database_t* ptr) {
                    auto target = ptr->address();
                    databases_.emplace(database_name, target);
                    addresses.push_back(target);
                },
                std::string(database_name), log_, 1, 1000);
        }
        actor_zeta::send(current_message()->sender(), address(), handler_id(route::create_databases_finish), session, addresses);
    }

    void manager_database_t::create(session_id_t& session, database_name_t& name) {
        trace(log_, "manager_database_t:create {}", name);
        spawn_supervisor<database_t>(
            [this, name, session](database_t* ptr) {
                auto target = ptr->address();
                databases_.emplace(name, target);
                auto self = this->address();
                return actor_zeta::send(current_message()->sender(), self, handler_id(route::create_database_finish), session, database_create_result(true), std::string(name), target);
            },
            std::string(name), log_, 1, 1000);
    }

    database_t::database_t(manager_database_t* supervisor, database_name_t name, log_t& log, size_t num_workers, size_t max_throughput)
        : actor_zeta::cooperative_supervisor<database_t>(supervisor, std::string(name))
        , name_(name)
        , log_(log.clone())
        , e_(supervisor->scheduler()) {
        ZoneScoped;
        add_handler(handler_id(route::create_collections), &database_t::create_collections);
        add_handler(handler_id(route::create_collection), &database_t::create);
        add_handler(handler_id(route::drop_collection), &database_t::drop);
    }

    database_t::~database_t() {
        ZoneScoped;
    }
    auto database_t::scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* {
        return e_;
    }

    //NOTE: behold thread-safety!
    auto database_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        ZoneScoped;
        std::unique_lock<spin_lock> _(lock_);
        set_current_message(std::move(msg));
        execute(this, current_message());
    }

    void database_t::create_collections(components::session::session_id_t &session, std::vector<collection_name_t> &collections,
                                        actor_zeta::address_t manager_disk) {
        debug(log_, "database_t::create_collections, database: {}, session: {}, count: {}", type(), session.data(), collections.size());
        std::vector<actor_zeta::address_t> addresses;
        for (const auto &collection : collections) {
            auto address = spawn_actor<collection::collection_t>(
                [this, &collection, &addresses](collection::collection_t* ptr) {
                    collections_.emplace(collection, ptr);
                    addresses.push_back(ptr->address());
                },
                collection, log_, manager_disk);
        }
        actor_zeta::send(current_message()->sender(), address(), handler_id(route::create_collections_finish), session, name_, addresses);
    }

    void database_t::create(session_id_t& session, collection_name_t& name, actor_zeta::address_t mdisk) {
        debug(log_, "database_t::create {}", name);
        auto address = spawn_actor<collection::collection_t>(
            [this, name](collection::collection_t* ptr) {
                collections_.emplace(name, ptr);
            },
            std::string(name), log_, mdisk);
        return actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::create_collection_finish), session, collection_create_result(true),std::string(name_), std::string(name), address);
    }

    void database_t::drop(components::session::session_id_t& session, collection_name_t& name) {
        debug(log_, "database_t::drop {}", name);
        auto self = this->address();
        auto collection = collections_.find(name);
        if (collection != collections_.end()) {
            auto target = collection->second->address();
            collections_.erase(collection);
            return actor_zeta::send(current_message()->sender(), self, handler_id(route::drop_collection_finish), session, result_drop_collection(true), std::string(name_),std::string(name), target);
        }
        return actor_zeta::send(current_message()->sender(), self, handler_id(route::drop_collection_finish), session, result_drop_collection(false), std::string(name_),std::string(name), self);
    }

    const database_name_t& database_t::name() {
        return name_;
    }
} // namespace services::database