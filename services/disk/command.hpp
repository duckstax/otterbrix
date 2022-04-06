#pragma once
#include <components/protocol/base.hpp>
#include <components/session/session.hpp>

namespace services::disk {

    struct command_write_documents_t {
        database_name_t database;
        collection_name_t collection;
        std::vector<components::document::document_ptr> documents;
    };

    struct command_remove_documents_t {
        database_name_t database;
        collection_name_t collection;
        std::vector<components::document::document_id_t> documents;
    };


    using command_t = std::variant<command_write_documents_t,
                                   command_remove_documents_t>;

    using command_storage_t = std::unordered_map<components::session::session_id_t, std::vector<command_t>>;

    void append_command(command_storage_t &storage, const components::session::session_id_t &session, const command_t &command);

} //namespace services::disk