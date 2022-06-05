#include "create_collection.hpp"

namespace components::protocol {

    create_collection_t::create_collection_t(const database_name_t& database, const collection_name_t &collection)
        : statement_t(statement_type::create_collection, database, collection) {
    }

} // namespace components::protocol
