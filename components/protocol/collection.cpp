#include "collection.hpp"

namespace components::protocol {

    statement_collection_t::statement_collection_t(statement_type type, const database_name_t& database, const collection_name_t &collection)
        : statement_t(type, database, collection) {
    }

    create_collection_t::create_collection_t(const database_name_t& database, const collection_name_t &collection)
        : statement_collection_t(statement_type::create_collection, database, collection) {
    }

    drop_collection_t::drop_collection_t(const database_name_t& database, const collection_name_t &collection)
        : statement_collection_t(statement_type::drop_collection, database, collection) {
    }

} // namespace components::protocol
