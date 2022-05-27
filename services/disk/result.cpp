#include "result.hpp"

namespace services::disk {

    std::vector<collection_name_t> result_database_t::name_collections() const {
        std::vector<collection_name_t> names(collections.size());
        std::size_t i = 0;
        for (const auto &collection : collections) {
            names[i++] = collection.name;
        }
        return names;
    }

    void result_database_t::set_collection(const std::vector<collection_name_t> &names) {
        collections.resize(names.size());
        std::size_t i = 0;
        for (const auto &name : names) {
            collections[i++] = {name, {}};
        }
    }


    result_load_t::result_load_t(const std::vector<database_name_t> &databases, wal::id_t wal_id)
        : wal_id_(wal_id) {
        databases_.resize(databases.size());
        std::size_t i = 0;
        for (const auto &database : databases) {
            databases_[i++] = {database, {}};
        }
    }

    const result_load_t::result_t &result_load_t::operator*() const {
        return databases_;
    }

    std::vector<database_name_t> result_load_t::name_databases() const {
        std::vector<database_name_t> names(databases_.size());
        std::size_t i = 0;
        for (const auto &database : databases_) {
            names[i++] = database.name;
        }
        return names;
    }

    std::size_t result_load_t::count_collections() const {
        std::size_t count = 0;
        for (const auto &database : databases_) {
            count += database.collections.size();
        }
        return count;
    }

    void result_load_t::clear() {
        databases_.clear();
        wal_id_ = 0;
    }

    wal::id_t result_load_t::wal_id() const {
        return wal_id_;
    }

    result_load_t::result_t &result_load_t::operator*() {
        return databases_;
    }

} // namespace services::disk