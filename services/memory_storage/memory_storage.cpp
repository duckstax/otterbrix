#include "memory_storage.hpp"
#include <cassert>
#include <core/tracy/tracy.hpp>
#include <core/system_command.hpp>
#include <components/ql/statements/create_collection.hpp>
#include <components/ql/statements/create_database.hpp>
#include <components/ql/statements/drop_collection.hpp>
#include <components/ql/statements/drop_database.hpp>
#include <services/collection/collection.hpp>
#include "route.hpp"

using namespace components::result;

namespace services {

    using namespace services::memory_storage;

    memory_storage_t::load_buffer_t::load_buffer_t(std::pmr::memory_resource* resource)
        : collections(resource) {
    }


    memory_storage_t::memory_storage_t(actor_zeta::detail::pmr::memory_resource* resource, actor_zeta::scheduler_raw scheduler, log_t& log)
        : actor_zeta::cooperative_supervisor<memory_storage_t>(resource, "memory_storage")
        , log_(log.clone())
        , e_(scheduler)
        , databases_(resource)
        , collections_(resource)
        , sync_(resource(),core::handler_id(core::route::sync),this, &memory_storage_t::sync)
        , execute_plan_(resource(),handler_id(route::execute_plan),this &memory_storage_t::execute_plan)
        , load_(resource(),handler_id(route::load),this &memory_storage_t::load)
        , drop_collection_finish_(resource(),collection::handler_id(collection::route::drop_collection_finish),this &memory_storage_t::drop_collection_finish_)
        , create_documents_finish_(resource(),collection::handler_id(collection::route::create_documents_finish),this &memory_storage_t::create_documents_finish_)
        , execute_plan_finish_(resource(),collection::handler_id(collection::route::execute_plan_finish),this &memory_storage_t::execute_plan_finish_) {
    }

    memory_storage_t::~memory_storage_t() {
        ZoneScoped;
        trace(log_, "delete memory_resource");
    }

    void memory_storage_t::sync(const address_pack& pack) {
        manager_dispatcher_ = std::get<static_cast<uint64_t>(unpack_rules::manager_dispatcher)>(pack);
        manager_disk_ = std::get<static_cast<uint64_t>(unpack_rules::manager_disk)>(pack);
    }

    void memory_storage_t::execute_plan(components::session::session_id_t& session,
                                        components::logical_plan::node_ptr logical_plan,
                                        components::ql::storage_parameters parameters) {
        using components::logical_plan::node_type;

        switch (logical_plan->type()) {
        case node_type::create_database_t:
            create_database_(session, std::move(logical_plan));
            break;
        case node_type::drop_database_t:
            drop_database_(session, std::move(logical_plan));
            break;
        case node_type::create_collection_t:
            create_collection_(session, std::move(logical_plan));
            break;
        case node_type::drop_collection_t:
            drop_collection_(session, std::move(logical_plan));
            break;
        default:
            execute_plan_(session, std::move(logical_plan), std::move(parameters));
            break;
        }
    }

    void memory_storage_t::load(components::session::session_id_t& session, const disk::result_load_t& result) {
        trace(log_, "memory_storage_t:load");
        load_buffer_ = std::make_unique<load_buffer_t>(resource());
        auto count_collections = std::accumulate((*result).begin(), (*result).end(), 0ul, [](size_t sum, const disk::result_database_t& database) {
            return sum + database.collections.size();
        });
        if (count_collections > 0) {
            sessions_.emplace(session, session_t{nullptr, current_message()->sender(), count_collections});
        }
        for (const auto& database : (*result)) {
            debug(log_, "memory_storage_t:load:create_database: {}", database.name);
            databases_.insert(database.name);
            for (const auto& collection : database.collections) {
                debug(log_, "memory_storage_t:load:create_collection: {}", collection.name);
                collection_full_name_t name(database.name, collection.name);
                auto collection_address = spawn_actor<collection::collection_t>([this, &name](services::collection::collection_t* ptr) {
                    collections_.emplace(name, ptr);
                }, name, log_, manager_disk_);
                load_buffer_->collections.addresses.emplace_back(result_list_addresses_t::res_t{name, collection_address});
                debug(log_, "memory_storage_t:load:fill_documents: {}", collection.documents.size());
                actor_zeta::send(collection_address, address(), collection::handler_id(collection::route::create_documents), session, collection.documents);
            }
        }
        if (count_collections == 0) {
            actor_zeta::send(current_message()->sender(), address(), handler_id(route::load_finish), session, make_result(load_buffer_->collections));
            load_buffer_.reset();
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

    bool memory_storage_t::is_exists_database_(const database_name_t& name) const {
        return databases_.find(name) != databases_.end();
    }

    bool memory_storage_t::is_exists_collection_(const collection_full_name_t& name) const {
        return collections_.contains(name);
    }

    bool memory_storage_t::check_database_(components::session::session_id_t& session, const database_name_t& name) {
        if (!is_exists_database_(name)) {
            actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::execute_plan_finish), session,
                             make_error(error_code_t::database_not_exists, "database not exists"));
            return false;
        }
        return true;
    }

    bool memory_storage_t::check_collection_(components::session::session_id_t& session, const collection_full_name_t& name) {
        if (check_database_(session, name.database)) {
            if (!is_exists_collection_(name)) {
                actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::execute_plan_finish), session,
                                 make_error(error_code_t::collection_not_exists, "collection not exists"));
                return false;
            }
            return true;
        }
        return false;
    }

    void memory_storage_t::create_database_(components::session::session_id_t& session, components::logical_plan::node_ptr logical_plan) {
        trace(log_, "memory_storage_t:create_database {}", logical_plan->database_name());
        if (is_exists_database_(logical_plan->database_name())) {
            actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::execute_plan_finish), session,
                             make_error(error_code_t::database_already_exists, "database already exists"));
            return;
        }
        databases_.insert(logical_plan->database_name());
        actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::execute_plan_finish), session,
                         make_result(empty_result_t()));
    }

    void memory_storage_t::drop_database_(components::session::session_id_t& session, components::logical_plan::node_ptr logical_plan) {
        trace(log_, "memory_storage_t:drop_database {}", logical_plan->database_name());
        if (check_database_(session, logical_plan->database_name())) {
            databases_.erase(logical_plan->database_name());
            actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::execute_plan_finish), session,
                             make_result(empty_result_t()));
        }
    }

    void memory_storage_t::create_collection_(components::session::session_id_t& session, components::logical_plan::node_ptr logical_plan) {
        trace(log_, "memory_storage_t:create_collection {}", logical_plan->collection_full().to_string());
        if (check_database_(session, logical_plan->database_name())) {
            if (is_exists_collection_(logical_plan->collection_full())) {
                actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::execute_plan_finish), session,
                                 make_error(error_code_t::collection_already_exists, "collection already exists"));
                return;
            }
            auto address = spawn_actor<collection::collection_t>([this, &logical_plan](collection::collection_t* ptr) {
                collections_.emplace(logical_plan->collection_full(), ptr);
            }, logical_plan->collection_full(), log_, manager_disk_);
            actor_zeta::send(current_message()->sender(), this->address(), handler_id(route::execute_plan_finish), session,
                             make_result(result_address_t(std::move(address))));
        }
    }

    void memory_storage_t::drop_collection_(components::session::session_id_t& session, components::logical_plan::node_ptr logical_plan) {
        trace(log_, "memory_storage_t:drop_collection {}", logical_plan->collection_full().to_string());
        if (check_collection_(session, logical_plan->collection_full())) {
            sessions_.emplace(session, session_t{logical_plan, current_message()->sender(), 1});
            actor_zeta::send(collections_.at(logical_plan->collection_full()), address(), collection::handler_id(collection::route::drop_collection), session);
        }
    }

    void memory_storage_t::execute_plan_(components::session::session_id_t& session,
                                         components::logical_plan::node_ptr logical_plan,
                                         components::ql::storage_parameters parameters) {
        trace(log_, "memory_storage_t:execute_plan {}", logical_plan->collection_full().to_string());
        if (check_collection_(session, logical_plan->collection_full())) {
            sessions_.emplace(session, session_t{logical_plan, current_message()->sender(), 1});
            actor_zeta::send(collections_.at(logical_plan->collection_full()),
                             address(),
                             collection::handler_id(collection::route::execute_plan),
                             session,
                             logical_plan,
                             std::move(parameters));
        }
    }

    void memory_storage_t::execute_plan_finish_(components::session::session_id_t& session, components::result::result_t result) {
        auto &s = sessions_.at(session);
        debug(log_, "memory_storage_t:execute_plan_finish: {}, success: {}", session.data(), result.is_success());
        actor_zeta::send(s.sender, address(), handler_id(route::execute_plan_finish), session, std::move(result));
        sessions_.erase(session);
    }

    void memory_storage_t::drop_collection_finish_(components::session::session_id_t& session, result_drop_collection& result) {
        const auto &s = sessions_.at(session);
        trace(log_, "memory_storage_t:drop_collection_finish {}", s.logical_plan->collection_full().to_string());
        if (result.is_success()) {
            collections_.erase(s.logical_plan->collection_full());
            actor_zeta::send(s.sender, address(), handler_id(route::execute_plan_finish), session, make_result(empty_result_t()));
        } else {
            actor_zeta::send(s.sender, address(), handler_id(route::execute_plan_finish), session,
                             make_error(error_code_t::other_error, "collection not dropped"));
        }
        sessions_.erase(session);
    }

    void memory_storage_t::create_documents_finish_(components::session::session_id_t& session) {
        if (!sessions_.contains(session)) {
            return;
        }
        auto &s = sessions_.at(session);
        --s.count_answers;
        debug(log_, "memory_storage_t:create_documents_finish: {}, rest: {}", session.data(), s.count_answers);
        if (s.count_answers == 0) {
            actor_zeta::send(s.sender, address(), handler_id(route::load_finish), session, make_result(load_buffer_->collections));
            load_buffer_.reset();
            sessions_.erase(session);
        }
    }

} // namespace services
