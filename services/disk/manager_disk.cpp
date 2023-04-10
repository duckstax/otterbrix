#include "manager_disk.hpp"
#include <core/system_command.hpp>
#include <components/index/disk/route.hpp>
#include "route.hpp"
#include "result.hpp"

namespace services::disk {

    using components::document::document_id_t;

    base_manager_disk_t::base_manager_disk_t(actor_zeta::detail::pmr::memory_resource* mr, actor_zeta::scheduler_raw scheduler)
        : actor_zeta::cooperative_supervisor<base_manager_disk_t>(mr, "manager_disk")
        , e_(scheduler) {
    }

    auto base_manager_disk_t::scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* {
        return e_;
    }

    auto base_manager_disk_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        set_current_message(std::move(msg));
        execute(this, current_message());
    }


    manager_disk_t::manager_disk_t(actor_zeta::detail::pmr::memory_resource* mr, actor_zeta::scheduler_raw scheduler, configuration::config_disk config, log_t& log)
        : base_manager_disk_t(mr, scheduler)
        , log_(log.clone())
        , config_(std::move(config)) {
        trace(log_, "manager_disk start");
        add_handler(core::handler_id(core::route::sync), &manager_disk_t::sync);
        add_handler(handler_id(route::create_agent), &manager_disk_t::create_agent);
        add_handler(handler_id(route::load), &manager_disk_t::load);
        add_handler(handler_id(route::append_database), &manager_disk_t::append_database);
        add_handler(handler_id(route::remove_database), &manager_disk_t::remove_database);
        add_handler(handler_id(route::append_collection), &manager_disk_t::append_collection);
        add_handler(handler_id(route::remove_collection), &manager_disk_t::remove_collection);
        add_handler(handler_id(route::write_documents), &manager_disk_t::write_documents);
        add_handler(handler_id(route::remove_documents), &manager_disk_t::remove_documents);
        add_handler(handler_id(route::flush), &manager_disk_t::flush);
        add_handler(handler_id(index::route::create), &manager_disk_t::create_index_agent);
        add_handler(handler_id(index::route::drop), &manager_disk_t::drop_index_agent);
        add_handler(handler_id(index::route::success), &manager_disk_t::drop_index_agent_success);
        trace(log_, "manager_disk finish");
    }

    manager_disk_t::~manager_disk_t() {
        trace(log_, "delete manager_disk_t");
    }

    void manager_disk_t::create_agent() {
        auto name_agent = "agent_disk_" + std::to_string(agents_.size() + 1);
        trace(log_, "manager_disk create_agent : {}", name_agent);
        auto address = spawn_actor<agent_disk_t>(
            [this](agent_disk_t* ptr) {
                agents_.emplace_back(agent_disk_ptr(ptr));
            },
            config_.path, name_agent, log_);
    }

    auto manager_disk_t::load(session_id_t& session) -> void {
        trace(log_, "manager_disk_t::load , session : {}", session.data());
        actor_zeta::send(agent(), address(), handler_id(route::load), session, current_message()->sender());
    }

    auto manager_disk_t::append_database(session_id_t& session, const database_name_t& database) -> void {
        trace(log_, "manager_disk_t::append_database , session : {} , database : {}", session.data(), database);
        command_append_database_t command{database};
        append_command(commands_, session, command_t(command));
    }

    auto manager_disk_t::remove_database(session_id_t& session, const database_name_t& database) -> void {
        trace(log_, "manager_disk_t::remove_database , session : {} , database : {}", session.data(), database);
        command_remove_database_t command{database};
        append_command(commands_, session, command_t(command));
    }

    auto manager_disk_t::append_collection(session_id_t& session, const database_name_t& database, const collection_name_t& collection) -> void {
        trace(log_, "manager_disk_t::append_collection , session : {} , database : {} , collection : {}", session.data(), database, collection);
        command_append_collection_t command{database, collection};
        append_command(commands_, session, command_t(command));
    }

    auto manager_disk_t::remove_collection(session_id_t& session, const database_name_t& database, const collection_name_t& collection) -> void {
        trace(log_, "manager_disk_t::remove_collection , session : {} , database : {} , collection : {}", session.data(), database, collection);
        command_remove_collection_t command{database, collection};
        append_command(commands_, session, command_t(command));
    }

    auto manager_disk_t::write_documents(session_id_t& session, const database_name_t& database, const collection_name_t& collection, const std::pmr::vector<document_ptr>& documents) -> void {
        trace(log_, "manager_disk_t::write_documents , session : {} , database : {} , collection : {}", session.data(), database, collection);
        command_write_documents_t command{database, collection, documents};
        append_command(commands_, session, command_t(command));
    }

    auto manager_disk_t::remove_documents(session_id_t& session, const database_name_t& database, const collection_name_t& collection, const std::pmr::vector<document_id_t>& documents) -> void {
        trace(log_, "manager_disk_t::remove_documents , session : {} , database : {} , collection : {}", session.data(), database, collection);
        command_remove_documents_t command{database, collection, documents};
        append_command(commands_, session, command_t(command));
    }

    auto manager_disk_t::flush(session_id_t& session, wal::id_t wal_id) -> void {
        trace(log_, "manager_disk_t::flush , session : {} , wal_id : {}", session.data(), wal_id);
        auto it = commands_.find(session);
        if (it != commands_.end()) {
            for (const auto& command : commands_.at(session)) {
                actor_zeta::send(agent(), address(), command.name(), command);
            }
            commands_.erase(session);
        }
        actor_zeta::send(agent(), address(), handler_id(route::fix_wal_id), wal_id);
    }

    void manager_disk_t::create_index_agent(session_id_t& session, const collection_name_t &collection_name, const index_name_t &index_name, components::ql::index_compare compare_type) {
        if (index_agents_.contains(index_name)) {
            error(log_, "manager_disk: index {} already exists", index_name);
            actor_zeta::send(current_message()->sender(), address(), index::handler_id(index::route::error), session);
        } else {
            trace(log_, "manager_disk: create_index_agent : {}", index_name);
            auto address_agent = spawn_actor<index_agent_disk_t>(
                [&](index_agent_disk_t* ptr) {
                    index_agents_.insert_or_assign(index_name, index_agent_disk_ptr(ptr));
                },
                resource(), config_.path, collection_name, index_name, compare_type, log_);
            actor_zeta::send(current_message()->sender(), address(), index::handler_id(index::route::success_create), session, address_agent);
        }
    }

    void manager_disk_t::drop_index_agent(session_id_t& session, const index_name_t &index_name) {
        if (index_agents_.contains(index_name)) {
            trace(log_, "manager_disk: drop_index_agent : {}", index_name);
            command_drop_index_t command{index_name, current_message()->sender()};
            append_command(commands_, session, command_t(command));
            actor_zeta::send(index_agents_.at(index_name)->address(), address(), index::handler_id(index::route::drop), session);
        } else {
            error(log_, "manager_disk: index {} already exists", index_name);
            actor_zeta::send(current_message()->sender(), address(), index::handler_id(index::route::error), session);
        }
    }

    void manager_disk_t::drop_index_agent_success(session_id_t& session) {
        auto it = commands_.find(session);
        if (it != commands_.end()) {
            for (const auto& command : commands_.at(session)) {
                auto command_drop = command.get<command_drop_index_t>();
                index_agents_.erase(command_drop.index_name);
                trace(log_, "manager_disk: drop_index_agent : {} : success", command_drop.index_name);
                actor_zeta::send(command_drop.address, address(), index::handler_id(index::route::success), session);
            }
            commands_.erase(session);
        }
    }

    auto manager_disk_t::agent() -> actor_zeta::address_t {
        return agents_[0]->address();
    }


    manager_disk_empty_t::manager_disk_empty_t(actor_zeta::detail::pmr::memory_resource* mr, actor_zeta::scheduler_raw scheduler)
        : base_manager_disk_t(mr, scheduler) {
        add_handler(core::handler_id(core::route::sync), &manager_disk_empty_t::nothing<std::tuple<actor_zeta::address_t, actor_zeta::address_t>>);
        add_handler(handler_id(route::create_agent), &manager_disk_empty_t::nothing<>);
        add_handler(handler_id(route::load), &manager_disk_empty_t::load);
        add_handler(handler_id(route::append_database), &manager_disk_empty_t::nothing<session_id_t&, const database_name_t&>);
        add_handler(handler_id(route::remove_database), &manager_disk_empty_t::nothing<session_id_t&, const database_name_t&>);
        add_handler(handler_id(route::append_collection), &manager_disk_empty_t::nothing<session_id_t&, const database_name_t&, const collection_name_t&>);
        add_handler(handler_id(route::remove_collection), &manager_disk_empty_t::nothing<session_id_t&, const database_name_t&, const collection_name_t&>);
        add_handler(handler_id(route::write_documents), &manager_disk_empty_t::nothing<session_id_t&, const database_name_t&, const collection_name_t&, const std::vector<document_ptr>&>);
        add_handler(handler_id(route::remove_documents), &manager_disk_empty_t::nothing<session_id_t&, const database_name_t&, const collection_name_t&, const std::vector<document_id_t>&>);
        add_handler(handler_id(route::flush), &manager_disk_empty_t::nothing<session_id_t&, wal::id_t>);
        add_handler(handler_id(index::route::create), &manager_disk_empty_t::create_index_agent);
        add_handler(handler_id(index::route::drop), &manager_disk_empty_t::nothing<session_id_t&, const index_name_t&>);
    }

    auto manager_disk_empty_t::load(session_id_t& session) -> void {
        auto result = result_load_t::empty();
        actor_zeta::send(current_message()->sender(), address(), handler_id(route::load_finish), session, result);
    }

    void manager_disk_empty_t::create_index_agent(session_id_t& session, const collection_name_t&, const index_name_t&, components::ql::index_compare) {
        actor_zeta::send(current_message()->sender(), address(), handler_id(index::route::success_create), session, actor_zeta::address_t::empty_address());
    }


} //namespace services::disk