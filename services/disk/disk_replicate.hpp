#pragma once

#include <goblin-engineer/core.hpp>
#include <log/log.hpp>
#include <components/session/session.hpp>
#include "disk.hpp"

namespace services::disk {

    class disk_manager_t final : public goblin_engineer::abstract_service {
        using session_t = ::components::session::session_t;

    public:
        disk_manager_t(goblin_engineer::supervisor_t *manager, const std::string &file_name, log_t& log);
        ~disk_manager_t() final;

        auto read_databases(session_t& session) -> void;
        auto append_database(session_t& session, const database_name_t &database) -> void;
        auto remove_database(session_t& session, const database_name_t &database) -> void;

        auto read_collections(session_t& session, const database_name_t &database) -> void;
        auto append_collection(session_t& session, const database_name_t &database, const collection_name_t &collection) -> void;
        auto remove_collection(session_t& session, const database_name_t &database, const collection_name_t &collection) -> void;

        auto read_documents(session_t& session, const database_name_t &database, const collection_name_t &collection) -> void;
        auto write_documents(session_t& session, const database_name_t &database, const collection_name_t &collection, const std::vector<document_ptr> &documents) -> void;
        auto remove_documents(session_t& session, const database_name_t &database, const collection_name_t &collection, const std::vector<document_id_t> &documents) -> void;

    private:
        log_t log_;
        disk_t disk_;
    };

} //namespace services::disk