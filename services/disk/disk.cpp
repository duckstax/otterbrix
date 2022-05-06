#include "disk.hpp"
#include <rocksdb/db.h>
#include <components/serialize/serialize.hpp>
#include "metadata.hpp"

namespace services::disk {

    const std::string key_separator = "::";
    const std::string key_structure = "structure";
    const std::string key_data = "data";

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

    bool statuses_ok(const std::vector<rocksdb::Status> &statuses) {
        for (const auto &status : statuses) {
            if (!status.ok()) {
                return false;
            }
        }
        return true;
    }


    disk_t::disk_t(const path_t& file_name)
        : db_(nullptr)
        , metadata_(nullptr)
        , file_wal_id_(nullptr) {
        rocksdb::Options options;
        options.IncreaseParallelism();
        options.OptimizeLevelStyleCompaction();
        options.create_if_missing = true;
        rocksdb::DB *db;
        auto status = rocksdb::DB::Open(options, file_name.string(), &db);
        if (status.ok()) {
            db_.reset(db);
            metadata_ = metadata_t::open(file_name / "METADATA");
            file_wal_id_ = std::make_unique<components::file::file_t>(file_name / "WAL_ID");
        } else {
            throw std::runtime_error("db open failed");
        }
    }

    disk_t::~disk_t() = default;

    void disk_t::save_document(const database_name_t &database, const collection_name_t &collection, const document_id_t& id, const document_ptr &document) {
        auto key = gen_key(database, collection, id);
        auto serialized_document = components::serialize::serialize(document);
        rocksdb::WriteBatch batch;
        batch.Put(gen_key(key_structure, key), to_string_view(serialized_document.structure));
        batch.Put(gen_key(key_data, key), to_string_view(serialized_document.data));
        db_->Write(rocksdb::WriteOptions(), &batch);
    }

    document_ptr disk_t::load_document(const rocks_id& id_rocks) const {
        std::vector<std::string> read_document(2);
        auto statuses = db_->MultiGet(rocksdb::ReadOptions(), {gen_key(key_structure, id_rocks), gen_key(key_data, id_rocks)}, &read_document);
        if (statuses_ok(statuses)) {
            components::serialize::serialized_document_t serialized_document;
            serialized_document.structure.write(read_document.at(0).data(), read_document.at(0).size());
            serialized_document.data.write(read_document.at(1).data(), read_document.at(1).size());
            return components::serialize::deserialize(serialized_document);
        }
        return nullptr;
    }

    document_ptr disk_t::load_document(const database_name_t &database, const collection_name_t &collection, const document_id_t& id) const {
        return load_document(gen_key(database, collection, id));
    }

    void disk_t::remove_document(const database_name_t &database, const collection_name_t &collection, const document_id_t &id) {
        auto key = gen_key(database, collection, id);
        rocksdb::WriteBatch batch;
        batch.Delete(gen_key(key_structure, key));
        batch.Delete(gen_key(key_data, key));
        db_->Write(rocksdb::WriteOptions(), &batch);
    }

    std::vector<rocks_id> disk_t::load_list_documents(const database_name_t &database, const collection_name_t &collection) const {
        std::vector<rocks_id> id_documents;
        rocksdb::Iterator* it = db_->NewIterator(rocksdb::ReadOptions());
        for (it->Seek(gen_key(key_structure, gen_key(database, collection)) + key_separator); it->Valid(); it->Next()) {
            id_documents.push_back(remove_prefix(it->key().ToString(), key_structure));
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

} //namespace services::disk