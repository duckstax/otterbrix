#include "metadata.hpp"
#include <algorithm>

namespace services::disk {

    metadata_t::packdata_t metadata_t::pack() const {
        packdata_t res;
        for (const auto& it : data_) {
            if (!res.empty()) {
                res += "\n";
            }
            res += it.first + ":";
            for (const auto &collection : it.second) {
                res += collection + ";";
            }
        }
        return res;
    }

    metadata_t::metadata_ptr metadata_t::extract(const metadata_t::packdata_t& package) {
        return std::make_unique<metadata_t>(package);
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

    void metadata_t::append_database(const metadata_t::database_name_t& database) {
        if (!is_exists_database(database)) {
            data_.insert_or_assign(database, collections_t());
        }
    }

    void metadata_t::remove_database(const metadata_t::database_name_t& database) {
        data_.erase(database);
    }

    bool metadata_t::is_exists_collection(const metadata_t::database_name_t& database, const metadata_t::collection_name_t& collection) const {
        auto it = data_.find(database);
        if (it != data_.end()) {
            return std::find(it->second.begin(), it->second.end(), collection) != it->second.end();
        }
        return false;
    }

    void metadata_t::append_collection(const metadata_t::database_name_t& database, const metadata_t::collection_name_t& collection) {
        if (!is_exists_collection(database, collection)) {
            auto it = data_.find(database);
            if (it != data_.end()) {
                it->second.push_back(collection);
            }
        }
    }

    void metadata_t::remove_collection(const metadata_t::database_name_t& database, const metadata_t::collection_name_t& collection) {
        auto it = data_.find(database);
        if (it != data_.end()) {
            it->second.erase(std::remove(it->second.begin(), it->second.end(), collection), it->second.end());
        }
    }

    metadata_t::metadata_t(const metadata_t::packdata_t& package) {
        auto trim = [](std::string &str) {
            str.erase(std::remove_if(str.begin(), str.end(), [](char c) {
                          return c == ';' || c == ':' || c == '\n';
                      }), str.end());
        };
        size_t pos = 0;
        database_name_t database;
        while (pos != std::string::npos) {
            auto pos_db = package.find(':', pos + 1);
            auto pos_col = package.find(';', pos + 1);
            if (pos_db < pos_col && pos_db != std::string::npos) {
                database = package.substr(pos, pos_db - pos);
                trim(database);
                append_database(database);
                pos = pos_db;
            } else if (pos_col != std::string::npos) {
                auto collection = package.substr(pos, pos_col - pos);
                trim(collection);
                append_collection(database, collection);
                pos = pos_col;
            } else {
                pos = std::string::npos;
            }
        }
    }
} //namespace services::disk