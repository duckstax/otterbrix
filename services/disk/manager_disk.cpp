#include "manager_disk.hpp"

#include <utility>
#include "route.hpp"
#include "result.hpp"

namespace services::disk {

    constexpr static auto name_dispatcher = "dispatcher";

    using components::document::document_id_t;

    manager_disk_t::manager_disk_t(path_t path_db, log_t& log, size_t num_workers, size_t max_throughput)
        : manager_t("manager_disk")
        , path_db_(std::move(path_db))
        , log_(log.clone())
        , e_(new goblin_engineer::shared_work(num_workers, max_throughput), goblin_engineer::detail::thread_pool_deleter()) {
        trace(log_, "manager_disk num_workers : {} , max_throughput: {}", num_workers, max_throughput);
        add_handler(route::create_agent, &manager_disk_t::create_agent);
        add_handler(route::read_databases, &manager_disk_t::read_databases);
        add_handler(route::append_database, &manager_disk_t::append_database);
        add_handler(route::remove_database, &manager_disk_t::remove_database);
        add_handler(route::read_collections, &manager_disk_t::read_collections);
        add_handler(route::append_collection, &manager_disk_t::append_collection);
        add_handler(route::remove_collection, &manager_disk_t::remove_collection);
        add_handler(route::read_documents, &manager_disk_t::read_documents);
        add_handler(route::write_documents, &manager_disk_t::write_documents);
        add_handler(route::remove_documents, &manager_disk_t::remove_documents);
        add_handler(route::flush, &manager_disk_t::flush);
        trace(log_, "manager_disk start thread pool");
        e_->start();
    }

    void manager_disk_t::create_agent() {
        auto name_agent = "agent_disk_" + std::to_string(agents_.size() + 1);
        trace(log_, "manager_disk create_agent : {}", name_agent);
        auto address = spawn_actor<agent_disk_t>(path_db_, name_agent, log_);
        agents_.emplace_back(address);
    }

    auto manager_disk_t::read_databases(session_id_t& session) -> void {
        trace(log_, "manager_disk_t::read_databases , session : {}", session.data());
        goblin_engineer::send(agent(), address(), route::read_databases, session);
    }

    auto manager_disk_t::append_database(session_id_t& session, const database_name_t &database) -> void {
        trace(log_, "manager_disk_t::append_database , session : {} , database : {}", session.data(), database);
        goblin_engineer::send(agent(), address(), route::append_database, database);
    }

    auto manager_disk_t::remove_database(session_id_t& session, const database_name_t &database) -> void {
        trace(log_, "manager_disk_t::remove_database , session : {} , database : {}", session.data(), database);
        goblin_engineer::send(agent(), address(), route::remove_database, database);
    }

    auto manager_disk_t::read_collections(session_id_t& session, const database_name_t &database) -> void {
        trace(log_, "manager_disk_t::read_collections , session : {} , database : {}", session.data(), database);
        goblin_engineer::send(agent(), address(), route::read_collections, session, database);
    }

    auto manager_disk_t::append_collection(session_id_t& session, const database_name_t &database, const collection_name_t &collection) -> void {
        trace(log_, "manager_disk_t::append_collection , session : {} , database : {} , collection : {}", session.data(), database, collection);
        goblin_engineer::send(agent(), address(), route::append_collection, database, collection);
    }

    auto manager_disk_t::remove_collection(session_id_t& session, const database_name_t &database, const collection_name_t &collection) -> void {
        trace(log_, "manager_disk_t::remove_collection , session : {} , database : {} , collection : {}", session.data(), database, collection);
        goblin_engineer::send(agent(), address(), route::remove_collection, database, collection);
    }

    auto manager_disk_t::read_documents(session_id_t& session, const database_name_t &database, const collection_name_t &collection) -> void {
        trace(log_, "manager_disk_t::read_documents , session : {} , database : {} , collection : {}", session.data(), database, collection);
        goblin_engineer::send(agent(), address(), route::read_documents, session, database, collection);
    }

    auto manager_disk_t::write_documents(session_id_t& session, const database_name_t &database, const collection_name_t &collection, const std::vector<document_ptr> &documents) -> void {
        trace(log_, "manager_disk_t::write_documents , session : {} , database : {} , collection : {}", session.data(), database, collection);
        command_write_documents_t command{database, collection, documents};
        append_command(commands_, session, command_t(command));
    }

    auto manager_disk_t::remove_documents(session_id_t& session, const database_name_t &database, const collection_name_t &collection, const std::vector<document_id_t> &documents) -> void {
        trace(log_, "manager_disk_t::remove_documents , session : {} , database : {} , collection : {}", session.data(), database, collection);
        command_remove_documents_t command{database, collection, documents};
        append_command(commands_, session, command_t(command));
    }

    auto manager_disk_t::flush(session_id_t& session) -> void {
        trace(log_, "manager_disk_t::flush , session : {}", session.data());
        for (const auto &command : commands_.at(session)) {
            goblin_engineer::send(agent(), address(), command.name(), command);
        }
        commands_.erase(session);
    }

    auto manager_disk_t::executor_impl() noexcept -> goblin_engineer::abstract_executor* {
        return e_.get();
    }

    auto manager_disk_t::enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void {
        set_current_message(std::move(msg));
        execute();
    }

    auto manager_disk_t::add_actor_impl(goblin_engineer::actor a) -> void {
        actor_storage_.emplace_back(std::move(a));
    }

    auto manager_disk_t::add_supervisor_impl(goblin_engineer::supervisor) -> void {
        log_.error("manager_disk_t::add_supervisor_impl");
    }

    auto manager_disk_t::agent() -> goblin_engineer::address_t& {
        return agents_[0];
    }


    agent_disk_t::agent_disk_t(goblin_engineer::supervisor_t *manager, const path_t &path_db, const name_t &name, log_t& log)
        : goblin_engineer::abstract_service(manager, name)
        , log_(log.clone())
        , disk_(path_db) {
        add_handler(route::read_databases, &agent_disk_t::read_databases);
        add_handler(route::append_database, &agent_disk_t::append_database);
        add_handler(route::remove_database, &agent_disk_t::remove_database);
        add_handler(route::read_collections, &agent_disk_t::read_collections);
        add_handler(route::append_collection, &agent_disk_t::append_collection);
        add_handler(route::remove_collection, &agent_disk_t::remove_collection);
        add_handler(route::read_documents, &agent_disk_t::read_documents);
        add_handler(route::write_documents, &agent_disk_t::write_documents);
        add_handler(route::remove_documents, &agent_disk_t::remove_documents);
    }

    agent_disk_t::~agent_disk_t() {
    }

    auto agent_disk_t::read_databases(session_id_t& session) -> void {
        trace(log_, "{}::read_databases , session : {}", type(), session.data());
        auto dispatcher = address_book(name_dispatcher);
        goblin_engineer::send(dispatcher, address(), route::read_databases_finish, session, result_read_databases(disk_.databases()));
    }

    auto agent_disk_t::append_database(const database_name_t &database) -> void {
        trace(log_, "{}::append_database , database : {}", type(), database);
        disk_.append_database(database);
    }

    auto agent_disk_t::remove_database(const database_name_t &database) -> void {
        trace(log_, "{}::remove_database , database : {}", type(), database);
        disk_.remove_database(database);
    }

    auto agent_disk_t::read_collections(session_id_t& session, const database_name_t &database) -> void {
        trace(log_, "{}::read_collections , session : {} , database : {}", type(), session.data(), database);
        auto dispatcher = address_book(name_dispatcher);
        goblin_engineer::send(dispatcher, address(), route::read_collections_finish, session, result_read_collections(disk_.collections(database)));
    }

    auto agent_disk_t::append_collection(const database_name_t &database, const collection_name_t &collection) -> void {
        trace(log_, "{}::append_collection , database : {} , collection : {}", type(), database, collection);
        disk_.append_collection(database, collection);
    }

    auto agent_disk_t::remove_collection(const database_name_t &database, const collection_name_t &collection) -> void {
        trace(log_, "{}::remove_collection , database : {} , collection : {}", type(), database, collection);
        disk_.remove_collection(database, collection);
    }

    auto agent_disk_t::read_documents(session_id_t& session, const database_name_t &database, const collection_name_t &collection) -> void {
        trace(log_, "{}::read_documents , session : {} , database : {} , collection : {}", type(), session.data(), database, collection);
        result_read_documents::result_t documents;
        for (const auto &id : disk_.load_list_documents(database, collection)) {
            documents.push_back(disk_.load_document(id));
        }
        auto dispatcher = address_book(name_dispatcher);
        goblin_engineer::send(dispatcher, address(), route::read_documents_finish, session, result_read_documents(std::move(documents)));
    }

    auto agent_disk_t::write_documents(const command_t &command) -> void {
        auto &write_command = command.get<command_write_documents_t>();
        trace(log_, "{}::write_documents , database : {} , collection : {} , {} documents", type(), write_command.database, write_command.collection, write_command.documents.size());
        for (const auto &document : write_command.documents) {
            auto id = components::document::get_document_id(document);
            if (!id.is_null()) {
                disk_.save_document(write_command.database, write_command.collection, id, document);
            }
        }
    }

    auto agent_disk_t::remove_documents(const command_t &command) -> void {
        auto &remove_command = command.get<command_remove_documents_t>();
        trace(log_, "{}::remove_documents , database : {} , collection : {} , {} documents", type(), remove_command.database, remove_command.collection, remove_command.documents.size());
        for (const auto &id : remove_command.documents) {
            disk_.remove_document(remove_command.database, remove_command.collection, id);
        }
    }

} //namespace services::disk