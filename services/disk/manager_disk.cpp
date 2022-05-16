#include "manager_disk.hpp"

#include <core/system_command.hpp>

#include "result.hpp"
#include "route.hpp"
#include <utility>

namespace services::disk {

    using components::document::document_id_t;

    manager_disk_t::manager_disk_t(actor_zeta::detail::pmr::memory_resource* mr,actor_zeta::scheduler_raw scheduler, path_t path_db, log_t& log)
        : actor_zeta::cooperative_supervisor<manager_disk_t>(mr, "manager_disk")
        , path_db_(std::move(path_db))
        , log_(log.clone())
        , e_(scheduler) {
        trace(log_, "manager_disk start");
        add_handler(handler_id(route::create_agent), &manager_disk_t::create_agent);
        add_handler(handler_id(route::read_databases), &manager_disk_t::read_databases);
        add_handler(handler_id(route::append_database), &manager_disk_t::append_database);
        add_handler(handler_id(route::remove_database), &manager_disk_t::remove_database);
        add_handler(handler_id(route::read_collections), &manager_disk_t::read_collections);
        add_handler(handler_id(route::append_collection), &manager_disk_t::append_collection);
        add_handler(handler_id(route::remove_collection), &manager_disk_t::remove_collection);
        add_handler(handler_id(route::read_documents), &manager_disk_t::read_documents);
        add_handler(handler_id(route::write_documents), &manager_disk_t::write_documents);
        add_handler(handler_id(route::remove_documents), &manager_disk_t::remove_documents);
        add_handler(handler_id(route::flush), &manager_disk_t::flush);
        add_handler(handler_id(route::load), &manager_disk_t::load);
        add_handler(core::handler_id(core::route::sync), &manager_disk_t::sync);
        trace(log_, "manager_disk finish");

    }

    void manager_disk_t::create_agent() {
        auto name_agent = "agent_disk_" + std::to_string(agents_.size() + 1);
        trace(log_, "manager_disk create_agent : {}", name_agent);
        auto address = spawn_actor<agent_disk_t>(
            [this](agent_disk_t* ptr) {
                agents_.emplace_back(ptr->address());
            },
            path_db_, name_agent, log_);
    }

    auto manager_disk_t::read_databases(session_id_t& session) -> void {
        trace(log_, "manager_disk_t::read_databases , session : {}", session.data());
        actor_zeta::send(agent(), current_message()->sender(), handler_id(route::read_databases), session);
    }

    auto manager_disk_t::append_database(session_id_t& session, const database_name_t& database) -> void {
        trace(log_, "manager_disk_t::append_database , session : {} , database : {}", session.data(), database);
        actor_zeta::send(agent(),  current_message()->sender(), handler_id(route::append_database), database);
    }

    auto manager_disk_t::remove_database(session_id_t& session, const database_name_t& database) -> void {
        trace(log_, "manager_disk_t::remove_database , session : {} , database : {}", session.data(), database);
        actor_zeta::send(agent(),  current_message()->sender(), handler_id(route::remove_database), database);
    }

    auto manager_disk_t::read_collections(session_id_t& session, const database_name_t& database) -> void {
        trace(log_, "manager_disk_t::read_collections , session : {} , database : {}", session.data(), database);
        actor_zeta::send(agent(),  current_message()->sender(), handler_id(route::read_collections), session, database);
    }

    auto manager_disk_t::append_collection(session_id_t& session, const database_name_t& database, const collection_name_t& collection) -> void {
        trace(log_, "manager_disk_t::append_collection , session : {} , database : {} , collection : {}", session.data(), database, collection);
        actor_zeta::send(agent(),  current_message()->sender(), handler_id(route::append_collection), database, collection);
    }

    auto manager_disk_t::remove_collection(session_id_t& session, const database_name_t& database, const collection_name_t& collection) -> void {
        trace(log_, "manager_disk_t::remove_collection , session : {} , database : {} , collection : {}", session.data(), database, collection);
        actor_zeta::send(agent(),  current_message()->sender(), handler_id(route::remove_collection), database, collection);
    }

    auto manager_disk_t::read_documents(session_id_t& session, const database_name_t& database, const collection_name_t& collection) -> void {
        trace(log_, "manager_disk_t::read_documents , session : {} , database : {} , collection : {}", session.data(), database, collection);
        actor_zeta::send(agent(),  current_message()->sender(), handler_id(route::read_documents), session, database, collection);
    }

    auto manager_disk_t::write_documents(session_id_t& session, const database_name_t& database, const collection_name_t& collection, const std::vector<document_ptr>& documents) -> void {
        trace(log_, "manager_disk_t::write_documents , session : {} , database : {} , collection : {}", session.data(), database, collection);
        command_write_documents_t command{database, collection, documents};
        append_command(commands_, session, command_t(command));
    }

    auto manager_disk_t::remove_documents(session_id_t& session, const database_name_t& database, const collection_name_t& collection, const std::vector<document_id_t>& documents) -> void {
        trace(log_, "manager_disk_t::remove_documents , session : {} , database : {} , collection : {}", session.data(), database, collection);
        command_remove_documents_t command{database, collection, documents};
        append_command(commands_, session, command_t(command));
    }

    auto manager_disk_t::flush(session_id_t& session, wal::id_t wal_id) -> void {
        trace(log_, "manager_disk_t::flush , session : {} , wal_id : {}", session.data(), wal_id);
        auto it = commands_.find(session);
        if (it != commands_.end()) {
            for (const auto& command : commands_.at(session)) {
                actor_zeta::send(agent(), current_message()->sender(), command.name(), command);
            }
            commands_.erase(session);
            actor_zeta::send(agent(), current_message()->sender(), handler_id(route::fix_wal_id), wal_id);
        }
    }

    void manager_disk_t::load(session_id_t &session) {
        trace(log_, "manager_disk_t::load , session : {}", session.data());
        actor_zeta::send(agent(), address(), handler_id(route::load), session, current_message()->sender());
    }

    auto manager_disk_t::scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* {
        return e_;
    }

    auto manager_disk_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        set_current_message(std::move(msg));
        execute(this, current_message());
    }

    auto manager_disk_t::agent() -> actor_zeta::address_t& {
        return agents_[0];
    }

    agent_disk_t::agent_disk_t(manager_disk_t* manager, const path_t& path_db, const name_t& name, log_t& log)
        : actor_zeta::basic_async_actor(manager, name)
        , log_(log.clone())
        , disk_(path_db) {
        add_handler(handler_id(route::append_database), &agent_disk_t::append_database);
        add_handler(handler_id(route::remove_database), &agent_disk_t::remove_database);
        add_handler(handler_id(route::append_collection), &agent_disk_t::append_collection);
        add_handler(handler_id(route::remove_collection), &agent_disk_t::remove_collection);
        add_handler(handler_id(route::write_documents), &agent_disk_t::write_documents);
        add_handler(handler_id(route::remove_documents), &agent_disk_t::remove_documents);
        add_handler(handler_id(route::fix_wal_id), &agent_disk_t::fix_wal_id);
        add_handler(handler_id(route::load), &agent_disk_t::load);
    }

    agent_disk_t::~agent_disk_t() {
    }

    auto agent_disk_t::append_database(const database_name_t& database) -> void {
        trace(log_, "{}::append_database , database : {}", type(), database);
        disk_.append_database(database);
    }

    auto agent_disk_t::remove_database(const database_name_t& database) -> void {
        trace(log_, "{}::remove_database , database : {}", type(), database);
        disk_.remove_database(database);
    }

    auto agent_disk_t::append_collection(const database_name_t& database, const collection_name_t& collection) -> void {
        trace(log_, "{}::append_collection , database : {} , collection : {}", type(), database, collection);
        disk_.append_collection(database, collection);
    }

    auto agent_disk_t::remove_collection(const database_name_t& database, const collection_name_t& collection) -> void {
        trace(log_, "{}::remove_collection , database : {} , collection : {}", type(), database, collection);
        disk_.remove_collection(database, collection);
    }

    auto agent_disk_t::write_documents(const command_t& command) -> void {
        auto& write_command = command.get<command_write_documents_t>();
        trace(log_, "{}::write_documents , database : {} , collection : {} , {} documents", type(), write_command.database, write_command.collection, write_command.documents.size());
        for (const auto& document : write_command.documents) {
            auto id = components::document::get_document_id(document);
            if (!id.is_null()) {
                disk_.save_document(write_command.database, write_command.collection, id, document);
            }
        }
    }

    auto agent_disk_t::remove_documents(const command_t& command) -> void {
        auto& remove_command = command.get<command_remove_documents_t>();
        trace(log_, "{}::remove_documents , database : {} , collection : {} , {} documents", type(), remove_command.database, remove_command.collection, remove_command.documents.size());
        for (const auto& id : remove_command.documents) {
            disk_.remove_document(remove_command.database, remove_command.collection, id);
        }
    }

    auto agent_disk_t::fix_wal_id(wal::id_t wal_id) -> void {
        trace(log_, "{}::fix_wal_id : {}", type(), wal_id);
        disk_.fix_wal_id(wal_id);
    }

    void agent_disk_t::load(session_id_t &session, actor_zeta::address_t dispatcher) {
        trace(log_, "{}::load , session : {}", type(), session.data());
//        auto databases = result_read_databases(disk_.databases());
//        actor_zeta::send(dispatcher, address(), handler_id(route::load_databases), session, databases);
//        for (const auto &database : databases.databases()) {
//            auto collections = result_read_collections(disk_.collections(database));
//            actor_zeta::send(dispatcher, address(), handler_id(route::load_collections), session, database, collections);
//            for (const auto &collection : collections.collections()) {
//                actor_zeta::send(dispatcher, address(), handler_id(route::load_documents), session, database, collection, result_read_collections(disk_.collections(database)));
//            }
//        }
    }

//    auto agent_disk_t::read_databases(session_id_t& session) -> void {
//        trace(log_, "{}::read_databases , session : {}", type(), session.data());
//        auto dispatcher = address_book(name_dispatcher);
//        goblin_engineer::send(dispatcher, address(), handler_id(route::read_databases_finish), session, result_read_databases(disk_.databases()));
//    }

//    auto agent_disk_t::read_collections(session_id_t& session, const database_name_t &database) -> void {
//        trace(log_, "{}::read_collections , session : {} , database : {}", type(), session.data(), database);
//        auto dispatcher = address_book(name_dispatcher);
//        goblin_engineer::send(dispatcher, address(), handler_id(route::read_collections_finish), session, result_read_collections(disk_.collections(database)));
//    }

//    auto agent_disk_t::read_documents(session_id_t& session, const database_name_t &database, const collection_name_t &collection) -> void {
//        trace(log_, "{}::read_documents , session : {} , database : {} , collection : {}", type(), session.data(), database, collection);
//        result_read_documents::result_t documents;
//        for (const auto &id : disk_.load_list_documents(database, collection)) {
//            documents.push_back(disk_.load_document(id));
//        }
//        auto dispatcher = address_book(name_dispatcher);
//        goblin_engineer::send(dispatcher, address(), handler_id(route::read_documents_finish), session, result_read_documents(std::move(documents)));
//    }

} //namespace services::disk