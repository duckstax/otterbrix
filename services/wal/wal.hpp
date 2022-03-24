#pragma once

#include <boost/filesystem.hpp>
#include <log/log.hpp>
#include "goblin-engineer/core.hpp"

#include <msgpack.hpp>
#include <msgpack/adaptor/vector.hpp>
#include <components/protocol/insert_many.hpp>

#include "dto.hpp"

class wal_replicate_t final : public goblin_engineer::abstract_service {
public:
    wal_replicate_t(goblin_engineer::supervisor_t* manager,log_t& log, boost::filesystem::path path);
    void create_database( std::string& name){}
    void create_collection( std::string& database_name, std::string& collections_name){}
    void drop_collection( std::string& database_name, std::string& collection_name){}
    void drop_database( std::string& database_name){}
    void insert_one( std::string& database_name, std::string& collection, components::document::document_t& document){}
    void insert_many( insert_many_t& data);
    void delete_one( std::string& database_name, std::string& collection, components::document::document_t& condition){}
    void delete_many( std::string& database_name, std::string& collection, components::document::document_t& condition){}
    void update_one( std::string& database_name, std::string& collection, components::document::document_t& condition, components::document::document_t update, bool upsert){}
    void update_many( std::string& database_name, std::string& collection, components::document::document_t& condition, components::document::document_t update, bool upsert){}
    void last_id() {}
    ~wal_replicate_t() override;
private:
    void write(){
        std::vector<iovec> iodata;
        iodata.emplace_back(iovec{buffer_.data(), buffer_.size()});
        int size_write = ::pwritev(fd_, iodata.data(), iodata.size(), writed_);
        writed_ += size_write;
        ++log_number_;
    };
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
    size_tt read_size(size_t start_index);
    buffer_t read(size_t start_index, size_t finish_index);
#endif
};