#include "metadata.hpp"
#include <algorithm>
#include <fstream>

namespace services::disk {

    metadata_t::metadata_ptr metadata_t::open(const path_t &file_name) {
        return std::unique_ptr<metadata_t>(new metadata_t(file_name));
    }

    metadata_t::databases_t metadata_t::databases() const {
        databases_t names;
        names.reserve(data_.size());
        for (const auto &it : data_) {
            names.emplace_back(it.first);
        }
        return names;
    }

    const metadata_t::collections_t &metadata_t::collections(const metadata_t::database_name_t& database) const {
        auto it = data_.find(database);
        if (it != data_.end()) {
            return it->second;
        }
        static const collections_t empty;
        return empty;
    }

    bool metadata_t::is_exists_database(const database_name_t &database) const {
        return data_.find(database) != data_.end();
    }

    bool metadata_t::append_database(const metadata_t::database_name_t& database, bool is_flush) {
        if (!is_exists_database(database)) {
            data_.insert_or_assign(database, collections_t());
            if (is_flush) {
                flush_();
            }
            return true;
        }
        return false;
    }

    bool metadata_t::remove_database(const metadata_t::database_name_t& database, bool is_flush) {
        bool result = data_.erase(database) > 0;
        if (is_flush && result) {
            flush_();
        }
        return result;
    }

    bool metadata_t::is_exists_collection(const metadata_t::database_name_t& database, const metadata_t::collection_name_t& collection) const {
        auto it = data_.find(database);
        if (it != data_.end()) {
            return std::find(it->second.begin(), it->second.end(), collection) != it->second.end();
        }
        return false;
    }

    bool metadata_t::append_collection(const metadata_t::database_name_t& database, const metadata_t::collection_name_t& collection, bool is_flush) {
        if (!is_exists_collection(database, collection)) {
            auto it = data_.find(database);
            if (it != data_.end()) {
                it->second.push_back(collection);
                if (is_flush) {
                    flush_();
                }
                return true;
            }
        }
        return false;
    }

    bool metadata_t::remove_collection(const metadata_t::database_name_t& database, const metadata_t::collection_name_t& collection, bool is_flush) {
        auto it = data_.find(database);
        if (it != data_.end()) {
            auto it_collection = std::remove(it->second.begin(), it->second.end(), collection);
            if (it_collection != it->second.end()) {
                it->second.erase(it_collection, it->second.end());
                if (is_flush) {
                    flush_();
                }
                return true;
            }
        }
        return false;
    }

    metadata_t::metadata_t(const path_t &file_name)
        : file_name_(file_name) {
        std::ifstream file(file_name.c_str());
        if (file.is_open()) {
            std::string line;
            while (std::getline(file, line)) {
                auto pos_db = line.find(':');
                if (pos_db != std::string::npos) {
                    auto database = line.substr(0, pos_db);
                    append_database(database, false);
                    auto pos = pos_db + 1;
                    auto pos_col = line.find(';', pos);
                    while (pos_col != std::string::npos) {
                        auto collection = line.substr(pos, pos_col - pos);
                        append_collection(database, collection, false);
                        pos = pos_col + 1;
                        pos_col = line.find(';', pos);
                    }
                }
            }
        }
        file.close();
    }

    void metadata_t::flush_() {
        std::ofstream file(file_name_.c_str());
        if (file.is_open()) {
            for (const auto& it : data_) {
                file << it.first + ":";
                for (const auto& collection : it.second) {
                    file << collection + ";";
                }
                file << "\n";
            }
        }
        file.close();
    }
} //namespace services::disk