#include "drop_collection.hpp"

namespace components::ql {

    drop_collection_t::drop_collection_t(const database_name_t& database, const collection_name_t &collection)
        : ql_statement_t(statement_type::drop_collection, database, collection) {
    }

} // namespace components::protocol
