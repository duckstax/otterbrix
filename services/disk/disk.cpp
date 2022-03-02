#include "disk.hpp"
#include <string_view>
#include <rocksdb/db.h>
#include <components/btree_storage/serialize.hpp>
#include <components/document/mutable/mutable_dict.h>
#include "metadata.hpp"

namespace services::disk {

    const std::string key_separator = "::";
    const std::string key_metadata = ".metadata";
    const std::string key_structure = "structure";
    const std::string key_data = "data";

    std::string gen_key(const std::string &key, const std::string &sub_key) {
        return key + key_separator + sub_key;
    }

    std::string gen_key(const std::string &database, const std::string &collection, const document_id_t &id) {
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


    disk_t::disk_t(const std::string_view& file_name)
        : db_(nullptr)
        , metadata_(nullptr) {
        rocksdb::Options options;
        options.IncreaseParallelism();
        options.OptimizeLevelStyleCompaction();
        options.create_if_missing = true;
        auto status = rocksdb::DB::Open(options, file_name.data(), &db_);
        if (status.ok()) {
            metadata_t::packdata_t buffer;
            status = db_->Get(rocksdb::ReadOptions(), key_metadata, &buffer);
            metadata_ = metadata_t::extract(status.ok() ? buffer : "");
        } else {
            throw std::runtime_error("db open failed");
        }
    }

    disk_t::~disk_t() {
        delete db_;
    }

    void disk_t::save_document(const std::string& database, const std::string& collection, const document_id_t& id, document_unique_ptr &&document) {
        if (db_) {
            auto key = gen_key(database, collection, id);
            auto serialized_document = components::btree::serialize(document);
            rocksdb::WriteBatch batch;
            batch.Put(gen_key(key_structure, key), to_string_view(serialized_document.structure));
            batch.Put(gen_key(key_data, key), to_string_view(serialized_document.data));
            db_->Write(rocksdb::WriteOptions(), &batch);
        }
    }

    document_unique_ptr disk_t::load_document(const std::string& id_rocks) const {
        if (db_) {
            std::vector<std::string> read_document(2);
            auto statuses = db_->MultiGet(rocksdb::ReadOptions(), {gen_key(key_structure, id_rocks), gen_key(key_data, id_rocks)}, &read_document);
            if (statuses_ok(statuses)) {
                components::btree::serialized_document_t serialized_document;
                serialized_document.structure.write(read_document.at(0).data(), read_document.at(0).size());
                serialized_document.data.write(read_document.at(1).data(), read_document.at(1).size());
                return components::btree::deserialize(serialized_document);
            }
        }
        return nullptr;
    }

    document_unique_ptr disk_t::load_document(const std::string& database, const std::string& collection, const document_id_t& id) const {
        return load_document(gen_key(database, collection, id));
    }

    void disk_t::remove_document(const std::string &database, const std::string &collection, const document_id_t &id) {
        if (db_) {
            auto key = gen_key(database, collection, id);
            rocksdb::WriteBatch batch;
            batch.Delete(gen_key(key_structure, key));
            batch.Delete(gen_key(key_data, key));
            db_->Write(rocksdb::WriteOptions(), &batch);
        }
    }

    std::vector<std::string> disk_t::load_list_documents(const std::string& database, const std::string& collection) const {
        std::vector<std::string> id_documents;
        if (db_) {
            rocksdb::Iterator* it = db_->NewIterator(rocksdb::ReadOptions());
            for (it->Seek(gen_key(key_structure, gen_key(database, collection)) + key_separator); it->Valid(); it->Next()) {
                id_documents.push_back(remove_prefix(it->key().ToString(), key_structure));
            }
            delete it;
        }
        return id_documents;
    }

    std::vector<std::string> disk_t::databases() const {
        if (metadata_) {
            return metadata_->databases();
        }
        return {};
    }

    void disk_t::append_database(const std::string& database) {
        if (metadata_ && !metadata_->is_exists_database(database)) {
            metadata_->append_database(database);
            flush_metadata();
        }
    }

    void disk_t::remove_database(const std::string& database) {
        if (metadata_ && metadata_->is_exists_database(database)) {
            metadata_->remove_database(database);
            flush_metadata();
        }
    }

    std::vector<std::string> disk_t::collections(const std::string &database) const {
        if (metadata_) {
            return metadata_->collections(database);
        }
        return {};
    }

    void disk_t::append_collection(const std::string& database, const std::string& collection) {
        if (metadata_ && !metadata_->is_exists_collection(database, collection)) {
            metadata_->append_collection(database, collection);
            flush_metadata();
        }
    }

    void disk_t::remove_collection(const std::string& database, const std::string& collection) {
        if (metadata_ && metadata_->is_exists_collection(database, collection)) {
            metadata_->remove_collection(database, collection);
            flush_metadata();
        }
    }

    void disk_t::flush_metadata() {
        if (db_) {
            db_->Put(rocksdb::WriteOptions(), key_metadata, metadata_->pack());
        }
    }

} //namespace services::disk