#pragma once

#include <goblin-engineer/core.hpp>
#include <log/log.hpp>
#include <components/session/session.hpp>
#include "disk.hpp"

namespace services::disk {

    class disk_replicate_t final : public goblin_engineer::abstract_service {
        using session_t = ::components::session::session_t;

    public:
        disk_replicate_t(goblin_engineer::supervisor_t *manager, const std::string_view &file_name, log_t& log);
        ~disk_replicate_t() final;

        auto read_databases(session_t& session) -> void;
        auto append_database(session_t& session, const std::string &database) -> void;
        auto remove_database(session_t& session, const std::string &database) -> void;

        auto read_collections(session_t& session, const std::string &database) -> void;
        auto append_collection(session_t& session, const std::string &database, const std::string &collection) -> void;
        auto remove_collection(session_t& session, const std::string &database, const std::string &collection) -> void;

        auto read_documents(session_t& session, const std::string &database, const std::string &collection) -> void;
        auto write_documents(session_t& session, const std::string &database, const std::string &collection, const std::vector<document_ptr> &documents) -> void;
        auto remove_documents(session_t& session, const std::string &database, const std::string &collection, const std::vector<document_id_t> &documents) -> void;

    private:
        log_t log_;
        disk_t disk_;
    };

} //namespace services::disk