#include "disk_replicate.hpp"
#include "route.hpp"
#include "result.hpp"

namespace services::disk {

    constexpr static char *name_dispatcher = "dispatcher";

    using components::document::document_id_t;

    disk_replicate_t::disk_replicate_t(goblin_engineer::supervisor_t *manager, const std::string_view &file_name, log_t& log)
        : goblin_engineer::abstract_service(manager, "disk")
        , log_(log.clone())
        , disk_(file_name) {
        add_handler(route::read_databases, &disk_replicate_t::read_databases);
        add_handler(route::append_database, &disk_replicate_t::append_database);
        add_handler(route::remove_database, &disk_replicate_t::remove_database);
        add_handler(route::read_collections, &disk_replicate_t::read_collections);
        add_handler(route::append_collection, &disk_replicate_t::append_collection);
        add_handler(route::remove_collection, &disk_replicate_t::remove_collection);
        add_handler(route::read_documents, &disk_replicate_t::read_documents);
        add_handler(route::write_documents, &disk_replicate_t::write_documents);
        add_handler(route::remove_documents, &disk_replicate_t::remove_documents);
    }

    disk_replicate_t::~disk_replicate_t() {
    }

    auto disk_replicate_t::read_databases(session_t& session) -> void {
        log_.debug("{}::read_databases", type());
        auto dispatcher = address_book(name_dispatcher);
        goblin_engineer::send(dispatcher, address(), route::read_databases_finish, session, result_read_databases(disk_.databases()));
    }

    auto disk_replicate_t::append_database(session_t& session, const std::string &database) -> void {
        log_.debug("{}::append_database(database={})", type(), database);
        auto dispatcher = address_book(name_dispatcher);
        goblin_engineer::send(dispatcher, address(), route::append_database_finish, session, result_success(disk_.append_database(database)));
    }

    auto disk_replicate_t::remove_database(session_t& session, const std::string &database) -> void {
        log_.debug("{}::remove_database(database={})", type(), database);
        auto dispatcher = address_book(name_dispatcher);
        goblin_engineer::send(dispatcher, address(), route::remove_database_finish, session, result_success(disk_.remove_database(database)));
    }

    auto disk_replicate_t::read_collections(session_t& session, const std::string &database) -> void {
        log_.debug("{}::read_collections(database={})", type(), database);
        auto dispatcher = address_book(name_dispatcher);
        goblin_engineer::send(dispatcher, address(), route::read_collections_finish, session, result_read_collections(disk_.collections(database)));
    }

    auto disk_replicate_t::append_collection(session_t& session, const std::string &database, const std::string &collection) -> void {
        log_.debug("{}::append_collection(database={}, collection={})", type(), database, collection);
        auto dispatcher = address_book(name_dispatcher);
        goblin_engineer::send(dispatcher, address(), route::append_collection_finish, session, result_success(disk_.append_collection(database, collection)));
    }

    auto disk_replicate_t::remove_collection(session_t& session, const std::string &database, const std::string &collection) -> void {
        log_.debug("{}::remove_collection(database={}, collection={})", type(), database, collection);
        auto dispatcher = address_book(name_dispatcher);
        goblin_engineer::send(dispatcher, address(), route::remove_collection_finish, session, result_success(disk_.remove_collection(database, collection)));
    }

    auto disk_replicate_t::read_documents(session_t& session, const std::string &database, const std::string &collection) -> void {
        log_.debug("{}::read_documents(database={}, collection={})", type(), database, collection);
        result_read_documents::result_t documents;
        for (const auto &id : disk_.load_list_documents(database, collection)) {
            documents.push_back(disk_.load_document(id));
        }
        auto dispatcher = address_book(name_dispatcher);
        goblin_engineer::send(dispatcher, address(), route::read_documents_finish, session, result_read_documents(std::move(documents)));
    }

    auto disk_replicate_t::write_documents(session_t& session, const std::string &database, const std::string &collection, const std::vector<document_ptr> &documents) -> void {
        log_.debug("{}::write_documents(database={}, collection={})", type(), database, collection);
        bool result = true;
        for (const auto &document : documents) {
            auto id = components::document::get_document_id(document);
            if (id.is_null()) {
                result = false;
            } else {
                disk_.save_document(database, collection, id, document);
            }
        }
        auto dispatcher = address_book(name_dispatcher);
        goblin_engineer::send(dispatcher, address(), route::write_documents_finish, session, result_success(result));
    }

    auto disk_replicate_t::remove_documents(session_t& session, const std::string &database, const std::string &collection, const std::vector<document_id_t> &documents) -> void {
        log_.debug("{}::remove_documents(database={}, collection={})", type(), database, collection);
        for (const auto &id : documents) {
            disk_.remove_document(database, collection, id);
        }
        auto dispatcher = address_book(name_dispatcher);
        goblin_engineer::send(dispatcher, address(), route::remove_documents_finish, session, result_success(true));
    }

} //namespace services::disk