#include "result.hpp"

namespace services::disk {

    void result_database_t::set_collection(const std::vector<collection_name_t> &names) {
        collections.resize(names.size());
        std::size_t i = 0;
        for (const auto &name : names) {
            collections[i++] = {name, {}};
        }
    }


    result_load_t::result_load_t(const std::vector<database_name_t> &databases) {
        databases_.resize(databases.size());
        std::size_t i = 0;
        for (const auto &database : databases) {
            databases_[i++] = {database, {}};
        }
    }

    const result_load_t::result_t &result_load_t::operator*() const {
        return databases_;
    }

    result_load_t::result_t &result_load_t::operator*() {
        return databases_;
    }

} // namespace services::disk