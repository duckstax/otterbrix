#pragma once

#include "goblin-engineer/core.hpp"
#include <boost/filesystem.hpp>
#include <log/log.hpp>

#include <components/protocol/insert_many.hpp>
#include <msgpack.hpp>
#include <msgpack/adaptor/vector.hpp>

#include "dto.hpp"

class wal_replicate_t final : public goblin_engineer::abstract_service {
public:
    wal_replicate_t(goblin_engineer::supervisor_t* manager, log_t& log, boost::filesystem::path path);
    void create_database(std::string& name) {}
    void create_collection(database_name_t& database, collection_name_t& collections) {}
    void drop_collection(database_name_t& database, collection_name_t& collection) {}
    void drop_database(database_name_t& database) {}
    void insert_one(database_name_t& database, collection_name_t& collection, components::document::document_t& document) {}
    void insert_many(insert_many_t& data);
    void delete_one(database_name_t& database, collection_name_t& collection, components::document::document_t& condition) {}
    void delete_many(database_name_t& database, collection_name_t& collection, components::document::document_t& condition) {}
    void update_one(database_name_t& database, collection_name_t& collection, components::document::document_t& condition, components::document::document_t update, bool upsert) {}
    void update_many(database_name_t& database, collection_name_t& collection, components::document::document_t& condition, components::document::document_t update, bool upsert) {}
    void last_id() {}
    ~wal_replicate_t() override;

private:
    void write_();
    bool file_exist_(boost::filesystem::path path);

    log_t log_;
    boost::filesystem::path path_;
    std::atomic<log_number_t> log_number_{0};
    crc32_t last_crc32_{0};
    std::size_t writed_{0};
    std::size_t read_{0};
    int fd_ = -1;
    buffer_t buffer_;
#ifdef DEV_MODE
public:
    std::size_t writed() const {
        return writed_;
    };
    size_tt read_size(size_t start_index);
    buffer_t read(size_t start_index, size_t finish_index);
#endif
};