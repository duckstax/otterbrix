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


    class command_t {
    public:
        template<class T>
        explicit command_t(T command)
            : data_(std::forward<T>(command)) {}

        template<class T>
        T& get() {
            return std::get<T>(data_);
        }

    private:
        std::variant<command_write_documents_t,
                     command_remove_documents_t> data_;
    };

    using command_storage_t = std::unordered_map<components::session::session_id_t, command_t>;

} //namespace services::disk