#include "memory_storage.hpp"
#include <cassert>
#include <core/tracy/tracy.hpp>
#include <core/system_command.hpp>
#include <components/ql/statements/create_database.hpp>
#include <components/ql/statements/drop_database.hpp>
#include <components/ql/statements/create_collection.hpp>
#include <components/ql/statements/drop_collection.hpp>
#include <services/collection/collection.hpp>
#include "result.hpp"
#include "route.hpp"

namespace services {

    using namespace services::memory_storage;

    memory_storage_t::memory_storage_t(actor_zeta::detail::pmr::memory_resource* resource, actor_zeta::scheduler_raw scheduler, log_t& log)
        : actor_zeta::cooperative_supervisor<memory_storage_t>(resource, "memory_storage")
        , log_(log.clone())
        , e_(scheduler)
        , databases_(resource)
        , collections_(resource) {
        ZoneScoped;
        trace(log_, "memory_storage start thread pool");
        add_handler(core::handler_id(core::route::sync), &memory_storage_t::sync);
        add_handler(handler_id(route::execute_ql), &memory_storage_t::execute_ql);
    }

    memory_storage_t::~memory_storage_t() {
        ZoneScoped;
        trace(log_, "delete memory_resource");
    }

    void memory_storage_t::sync(const address_pack& pack) {
        manager_dispatcher_ = std::get<static_cast<uint64_t>(unpack_rules::manager_dispatcher)>(pack);
        manager_disk_ = std::get<static_cast<uint64_t>(unpack_rules::manager_disk)>(pack);
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
            assert(false && "not valid ql statement");
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

    bool memory_storage_t::is_exists_database(const database_name_t& name) const {
        return databases_.find(name) != databases_.end();
    }

    bool memory_storage_t::is_exists_collection(const collection_full_name_t& name) const {
        return collections_.contains(name);
    }

    void memory_storage_t::create_database_(components::session::session_id_t& session, components::ql::create_database_t* ql) {
        trace(log_, "memory_storage_t:create_database {}", ql->database_);
        if (is_exists_database(ql->database_)) {
            actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::execute_ql_finish), session,
                             make_error(ql, empty_result_t(), "database already exists"));
            return;
        }
        databases_.insert(ql->database_);
        actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::execute_ql_finish), session,
                         make_result(ql, empty_result_t()));
    }

    void memory_storage_t::drop_database_(components::session::session_id_t& session, components::ql::drop_database_t* ql) {
        trace(log_, "memory_storage_t:drop_database {}", ql->database_);
        if (!is_exists_database(ql->database_)) {
            actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::execute_ql_finish), session,
                             make_error(ql, empty_result_t(), "database not exists"));
            return;
        }
        databases_.erase(ql->database_);
        actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::execute_ql_finish), session,
                         make_result(ql, empty_result_t()));
    }

    void memory_storage_t::create_collection_(components::session::session_id_t& session, components::ql::create_collection_t* ql) {
        collection_full_name_t name(ql->database_, ql->collection_);
        trace(log_, "memory_storage_t:create_collection {}", name.to_string());
        if (!is_exists_database(name.database)) {
            actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::execute_ql_finish), session,
                             make_error(ql, result_address_t(), "database not exists"));
            return;
        }
        if (is_exists_collection(name)) {
            actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::execute_ql_finish), session,
                             make_error(ql, result_address_t(), "collection already exists"));
            return;
        }
        auto address = spawn_actor<collection::collection_t>([this, &name](collection::collection_t* ptr) {
            collections_.emplace(name, ptr);
        }, name, log_, manager_disk_);
        actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::execute_ql_finish), session,
                         make_result(ql, result_address_t(std::move(address))));
    }

    void memory_storage_t::drop_collection_(components::session::session_id_t& session, components::ql::drop_collection_t* ql) {
        collection_full_name_t name(ql->database_, ql->collection_);
        trace(log_, "memory_storage_t:drop_collection {}", name.to_string());
        if (!is_exists_database(name.database)) {
            actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::execute_ql_finish), session,
                             make_error(ql, empty_result_t(), "database not exists"));
            return;
        }
        if (!is_exists_collection(name)) {
            actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::execute_ql_finish), session,
                             make_error(ql, empty_result_t(), "collection not exists"));
            return;
        }
        collections_.erase(name);
        actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::execute_ql_finish), session,
                         make_result(ql, empty_result_t()));
    }

} // namespace services
