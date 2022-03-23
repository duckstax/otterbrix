#pragma once
#include <components/document/document.hpp>
#include <components/document/document_id.hpp>

namespace rocksdb {
    class DB;
}

namespace services::disk {

    class metadata_t;

    using metadata_ptr = std::unique_ptr<metadata_t>;
    using components::document::document_ptr;
    using components::document::document_id_t;

    class disk_t {
    public:
        explicit disk_t(const std::string_view &file_name);
        disk_t(const disk_t &) = delete;
        disk_t &operator=(disk_t const&) = delete;
        ~disk_t();

        void save_document(const std::string &database, const std::string &collection, const document_id_t &id, const document_ptr &document);
        [[nodiscard]] document_ptr load_document(const std::string& id_rocks) const;
        [[nodiscard]] document_ptr load_document(const std::string& database, const std::string& collection, const document_id_t& id) const;
        void remove_document(const std::string &database, const std::string &collection, const document_id_t &id);
        [[nodiscard]] std::vector<std::string> load_list_documents(const std::string &database, const std::string &collection) const;

        [[nodiscard]] std::vector<std::string> databases() const;
        void append_database(const std::string &database);
        void remove_database(const std::string &database);

        [[nodiscard]] std::vector<std::string> collections(const std::string &database) const;
        void append_collection(const std::string &database, const std::string &collection);
        void remove_collection(const std::string &database, const std::string &collection);

    private:
        rocksdb::DB* db_;
        metadata_ptr metadata_;

        void flush_metadata();
    };

} //namespace services::disk
