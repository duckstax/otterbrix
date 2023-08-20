#include "memory_storage.hpp"
#include <cassert>
#include <core/tracy/tracy.hpp>
#include <core/system_command.hpp>
#include <components/ql/statements/create_database.hpp>
#include <components/ql/statements/drop_database.hpp>
#include <components/ql/statements/create_collection.hpp>
#include <components/ql/statements/drop_collection.hpp>
#include "result.hpp"
#include "route.hpp"

namespace services {

    memory_storage_t::memory_storage_t(actor_zeta::detail::pmr::memory_resource* resource, actor_zeta::scheduler_raw scheduler, log_t& log)
        : actor_zeta::cooperative_supervisor<memory_storage_t>(resource, "memory_storage")
        , log_(log.clone())
        , e_(scheduler)
        , databases_(resource)
        , collections_(resource) {
        ZoneScoped;
        trace(log_, "memory_storage start thread pool");
        add_handler(core::handler_id(core::route::sync), &memory_storage_t::sync);
        add_handler(memory_storage::handler_id(memory_storage::route::execute_ql), &memory_storage_t::execute_ql);
    }

    memory_storage_t::~memory_storage_t() {
        ZoneScoped;
        trace(log_, "delete memory_resource");
    }

    void memory_storage_t::sync(const address_pack& pack) {
        manager_dispatcher_ = std::get<static_cast<uint64_t>(unpack_rules::manager_dispatcher)>(pack);
    }

    void memory_storage_t::execute_ql(components::session::session_id_t& session, components::ql::ql_statement_t* ql) {
        using components::ql::statement_type;

        switch (ql->type()) {
        case statement_type::create_database:
            create_database_(session, static_cast<components::ql::create_database_t*>(ql));
            break;
        case statement_type::drop_database:
            drop_database_(session, static_cast<components::ql::drop_database_t*>(ql));
            break;
        case statement_type::create_collection:
            create_collection_(session, static_cast<components::ql::create_collection_t*>(ql));
            break;
        case statement_type::drop_collection:
            drop_collection_(session, static_cast<components::ql::drop_collection_t*>(ql));
            break;
        default:
            assert(false && "not valid statement");
            break;
        }
    }

    actor_zeta::scheduler::scheduler_abstract_t *memory_storage_t::scheduler_impl() noexcept {
        return e_;
    }

    void memory_storage_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) {
        ZoneScoped;
        std::unique_lock<spin_lock> _(lock_);
        set_current_message(std::move(msg));
        execute(this, current_message());
    }

    void memory_storage_t::create_database_(components::session::session_id_t& session, components::ql::create_database_t* ql) {
    }

    void memory_storage_t::drop_database_(components::session::session_id_t& session, components::ql::drop_database_t* ql) {
    }

    void memory_storage_t::create_collection_(components::session::session_id_t& session, components::ql::create_collection_t* ql) {
    }

    void memory_storage_t::drop_collection_(components::session::session_id_t& session, components::ql::drop_collection_t* ql) {
    }


//    void memory_storage_t::create_databases(session_id_t& session, std::vector<database_name_t>& databases) {
//        trace(log_, "manager_database_t:create_databases, session: {}, count: {}", session.data(), databases.size());
//        std::vector<actor_zeta::address_t> addresses;
//        for (const auto &database_name : databases) {
//            spawn_supervisor<database_t>(
//                [this, &database_name, &addresses](database_t* ptr) {
//                    auto target = ptr->address();
//                    databases_.emplace(database_name, target);
//                    addresses.push_back(target);
//                },
//                std::string(database_name), log_, 1, 1000);
//        }
//        actor_zeta::send(current_message()->sender(), address(), handler_id(route::create_databases_finish), session, addresses);
//    }

//    void memory_storage_t::create(session_id_t& session, components::ql::ql_statement_t* statement) {
//        trace(log_, "manager_database_t:create {}", statement->database_);
//        spawn_supervisor<database_t>(
//            [this, statement, session](database_t* ptr) {
//                auto target = ptr->address();
//                databases_.emplace(statement->database_, target);
//                auto self = this->address();
//                return actor_zeta::send(current_message()->sender(), self, handler_id(route::create_database_finish), session, database_create_result(true, statement, target));
//            },
//            statement->database_, log_, 1, 1000);
//    }

//    void memory_storage_t::create_collections(components::session::session_id_t &session, std::vector<collection_name_t> &collections,
//                                        actor_zeta::address_t manager_disk) {
//        debug(log_, "database_t::create_collections, database: {}, session: {}, count: {}", type(), session.data(), collections.size());
//        std::vector<actor_zeta::address_t> addresses;
//        for (const auto &collection : collections) {
//            auto address = spawn_actor<collection::collection_t>(
//                [this, &collection, &addresses](collection::collection_t* ptr) {
//                    collections_.emplace(collection, ptr);
//                    addresses.push_back(ptr->address());
//                },
//                collection, log_, manager_disk);
//        }
//        actor_zeta::send(current_message()->sender(), address(), handler_id(route::create_collections_finish), session, name_, addresses);
//    }

//    void memory_storage_t::create(session_id_t& session, components::ql::ql_statement_t* statement, actor_zeta::address_t mdisk) {
//        debug(log_, "database_t::create {}", statement->collection_);
//        auto address = spawn_actor<collection::collection_t>(
//            [this, statement](collection::collection_t* ptr) {
//                collections_.emplace(statement->collection_, ptr);
//            },
//            statement->collection_, log_, mdisk);
//        return actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::create_collection_finish), session, collection_create_result(true, statement, address));
//    }

//    void memory_storage_t::drop(components::session::session_id_t& session, collection_name_t& name) {
//        debug(log_, "database_t::drop {}", name);
//        auto self = this->address();
//        auto collection = collections_.find(name);
//        if (collection != collections_.end()) {
//            auto target = collection->second->address();
//            collections_.erase(collection);
//            return actor_zeta::send(current_message()->sender(), self, handler_id(route::drop_collection_finish), session, result_drop_collection(true), std::string(name_),std::string(name), target);
//        }
//        return actor_zeta::send(current_message()->sender(), self, handler_id(route::drop_collection_finish), session, result_drop_collection(false), std::string(name_),std::string(name), self);
//    }

} // namespace services
