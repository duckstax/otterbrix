#pragma once
#include <components/document/document.hpp>
#include <components/document/document_id.hpp>

namespace rocksdb {
    class DB;
}

namespace services::disk {

    class metadata_t;

    using rocks_id = std::string;
    using database_name_t = std::string;
    using collection_name_t = std::string;
    using metadata_ptr = std::unique_ptr<metadata_t>;
    using db_ptr = std::unique_ptr<rocksdb::DB>;
    using components::document::document_ptr;
    using components::document::document_id_t;

    class disk_t {
    public:
        explicit disk_t(const std::string &file_name);
        disk_t(const disk_t &) = delete;
        disk_t &operator=(disk_t const&) = delete;
        ~disk_t();

        void save_document(const database_name_t &database, const collection_name_t &collection, const document_id_t &id, const document_ptr &document);
        [[nodiscard]] document_ptr load_document(const rocks_id& id_rocks) const;
        [[nodiscard]] document_ptr load_document(const database_name_t &database, const collection_name_t &collection, const document_id_t& id) const;
        void remove_document(const database_name_t &database, const collection_name_t &collection, const document_id_t &id);
        [[nodiscard]] std::vector<rocks_id> load_list_documents(const database_name_t &database, const collection_name_t &collection) const;

        [[nodiscard]] std::vector<database_name_t> databases() const;
        bool append_database(const database_name_t &database);
        bool remove_database(const database_name_t &database);

        [[nodiscard]] std::vector<collection_name_t> collections(const database_name_t &database) const;
        bool append_collection(const database_name_t &database, const collection_name_t &collection);
        bool remove_collection(const database_name_t &database, const collection_name_t &collection);

    private:
        db_ptr db_;
        metadata_ptr metadata_;
    };

} //namespace services::disk
