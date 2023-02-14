#include "manager_disk.hpp"

#include <core/system_command.hpp>

#include "route.hpp"
#include "result.hpp"

namespace services::disk {

    using components::document::document_id_t;

    agent_disk_t::agent_disk_t(base_manager_disk_t* manager, const path_t& path_db, const name_t& name, log_t& log)
        : actor_zeta::basic_async_actor(manager, name)
        , log_(log.clone())
        , disk_(path_db) {
        trace(log_, "agent_disk::create");
        add_handler(handler_id(route::load), &agent_disk_t::load);
        add_handler(handler_id(route::append_database), &agent_disk_t::append_database);
        add_handler(handler_id(route::remove_database), &agent_disk_t::remove_database);
        add_handler(handler_id(route::append_collection), &agent_disk_t::append_collection);
        add_handler(handler_id(route::remove_collection), &agent_disk_t::remove_collection);
        add_handler(handler_id(route::write_documents), &agent_disk_t::write_documents);
        add_handler(handler_id(route::remove_documents), &agent_disk_t::remove_documents);
        add_handler(handler_id(route::fix_wal_id), &agent_disk_t::fix_wal_id);
    }

    agent_disk_t::~agent_disk_t() {
        trace(log_, "delete agent_disk_t");
    }

    auto agent_disk_t::load(session_id_t& session, actor_zeta::address_t dispatcher) -> void {
        trace(log_, "agent_disk::load , session : {}", session.data());
        result_load_t result(disk_.databases(), disk_.wal_id());
        for (auto &database : *result) {
            database.set_collection(disk_.collections(database.name));
            for (auto &collection : database.collections) {
                auto id_documents = disk_.load_list_documents(database.name, collection.name);
                for (const auto &id : id_documents) {
                    collection.documents.push_back(disk_.load_document(id));
                }
            }
        }
        actor_zeta::send(dispatcher, address(), handler_id(route::load_finish), session, result);
    }

    auto agent_disk_t::append_database(const command_t& command) -> void {
        auto& cmd = command.get<command_append_database_t>();
        trace(log_, "agent_disk::append_database , database : {}", cmd.database);
        disk_.append_database(cmd.database);
    }

    auto agent_disk_t::remove_database(const command_t& command) -> void {
        auto& cmd = command.get<command_remove_database_t>();
        trace(log_, "agent_disk::remove_database , database : {}", cmd.database);
        disk_.remove_database(cmd.database);
    }

    auto agent_disk_t::append_collection(const command_t& command) -> void {
        auto& cmd = command.get<command_append_collection_t>();
        trace(log_, "agent_disk::append_collection , database : {} , collection : {}", cmd.database, cmd.collection);
        disk_.append_collection(cmd.database, cmd.collection);
    }

    auto agent_disk_t::remove_collection(const command_t& command) -> void {
        auto& cmd = command.get<command_remove_collection_t>();
        trace(log_, "agent_disk::remove_collection , database : {} , collection : {}", cmd.database, cmd.collection);
        disk_.remove_collection(cmd.database, cmd.collection);
    }

    auto agent_disk_t::write_documents(const command_t& command) -> void {
        auto& write_command = command.get<command_write_documents_t>();
        trace(log_, "agent_disk::write_documents , database : {} , collection : {} , {} documents", write_command.database, write_command.collection, write_command.documents.size());
        for (const auto& document : write_command.documents) {
            auto id = components::document::get_document_id(document);
            if (!id.is_null()) {
                disk_.save_document(write_command.database, write_command.collection, id, document);
            }
        }
    }

    auto agent_disk_t::remove_documents(const command_t& command) -> void {
        auto& remove_command = command.get<command_remove_documents_t>();
        trace(log_, "agent_disk::remove_documents , database : {} , collection : {} , {} documents", remove_command.database, remove_command.collection, remove_command.documents.size());
        for (const auto& id : remove_command.documents) {
            disk_.remove_document(remove_command.database, remove_command.collection, id);
        }
    }

    auto agent_disk_t::fix_wal_id(wal::id_t wal_id) -> void {
        trace(log_, "agent_disk::fix_wal_id : {}", wal_id);
        disk_.fix_wal_id(wal_id);
    }


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
            actor_zeta::send(agent(), address(), handler_id(route::fix_wal_id), wal_id);
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
    }

    auto manager_disk_empty_t::load(session_id_t& session) -> void {
        auto result = result_load_t::empty();
        actor_zeta::send(current_message()->sender(), address(), handler_id(route::load_finish), session, result);

    }


} //namespace services::disk