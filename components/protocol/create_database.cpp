#include "create_database.hpp"

namespace components::ql {

    create_database_t::create_database_t(const database_name_t& database)
        : ql_statement_t(statement_type::create_database, database, collection_name_t()) {
    }

} // namespace components::protocol
