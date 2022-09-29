#include "disk.hpp"
#include <rocksdb/db.h>
#include <components/document/msgpack/msgpack_encoder.hpp>
#include "metadata.hpp"

namespace services::disk {

    const std::string key_separator = "::";

    std::string gen_key(const std::string &key, const std::string &sub_key) {
        return key + key_separator + sub_key;
    }

    std::string gen_key(const database_name_t &database, const collection_name_t &collection, const document_id_t &id) {
        return gen_key(gen_key(database, collection), id.to_string());
    }

    std::string remove_prefix(const std::string &key, const std::string &prefix) {
        return key.substr(prefix.size() + key_separator.size());
    }

    std::string_view to_string_view(const msgpack::sbuffer &buffer) {
        return std::string_view(buffer.data(), buffer.size());
    }


    disk_t::disk_t(const path_t& file_name)
        : db_(nullptr)
        , metadata_(nullptr)
        , file_wal_id_(nullptr) {
        rocksdb::Options options;
        //options.IncreaseParallelism();
        options.OptimizeLevelStyleCompaction();
        options.create_if_missing = true;
        rocksdb::DB *db;
        auto status = rocksdb::DB::Open(options, file_name.string(), &db);
        if (status.ok()) {
            db_.reset(db);
            metadata_ = metadata_t::open(file_name / "METADATA");
            file_wal_id_ = std::make_unique<core::file::file_t>(file_name / "WAL_ID");
        } else {
            throw std::runtime_error("db open failed");
        }
    }

    disk_t::~disk_t() = default;

    void disk_t::save_document(const database_name_t &database, const collection_name_t &collection, const document_id_t& id, const document_ptr &document) {
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, document);
        db_->Put(rocksdb::WriteOptions(), gen_key(database, collection, id), to_string_view(sbuf));
    }

    document_ptr disk_t::load_document(const rocks_id& id_rocks) const {
        std::string sbuf;
        auto status = db_->Get(rocksdb::ReadOptions(), id_rocks, &sbuf);
        if (status.ok()) {
            msgpack::unpacked msg;
            msgpack::unpack(msg, sbuf.data(), sbuf.size());
            msgpack::object obj = msg.get();
            return obj.as<document_ptr>();
        }
        return nullptr;
    }

    document_ptr disk_t::load_document(const database_name_t &database, const collection_name_t &collection, const document_id_t& id) const {
        return load_document(gen_key(database, collection, id));
    }

    void disk_t::remove_document(const database_name_t &database, const collection_name_t &collection, const document_id_t &id) {
        db_->Delete(rocksdb::WriteOptions(), gen_key(database, collection, id));
    }

    std::vector<rocks_id> disk_t::load_list_documents(const database_name_t &database, const collection_name_t &collection) const {
        std::vector<rocks_id> id_documents;
        rocksdb::Iterator* it = db_->NewIterator(rocksdb::ReadOptions());
        auto find_key = gen_key(database, collection) + key_separator;
        for (it->Seek(find_key); it->Valid() && it->key().starts_with(find_key); it->Next()) {
            id_documents.push_back(it->key().ToString());
        }
        delete it;
        return id_documents;
    }

    std::vector<database_name_t> disk_t::databases() const {
        return metadata_->databases();
    }

    bool disk_t::append_database(const database_name_t &database) {
        return metadata_->append_database(database);
    }

    bool disk_t::remove_database(const database_name_t &database) {
        return metadata_->remove_database(database);
    }

    std::vector<collection_name_t> disk_t::collections(const database_name_t &database) const {
        return metadata_->collections(database);
    }

    bool disk_t::append_collection(const database_name_t &database, const collection_name_t &collection) {
        return metadata_->append_collection(database, collection);
    }

    bool disk_t::remove_collection(const database_name_t &database, const collection_name_t &collection) {
        return metadata_->remove_collection(database, collection);
    }

    void disk_t::fix_wal_id(wal::id_t wal_id) {
        auto id = std::to_string(wal_id);
        file_wal_id_->rewrite(id);
    }

    wal::id_t disk_t::wal_id() const {
        return wal::id_from_string(file_wal_id_->readall());
    }

} //namespace services::disk