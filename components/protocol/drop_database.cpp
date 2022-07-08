#include "drop_database.hpp"

namespace components::protocol {

    drop_database_t::drop_database_t(const database_name_t& database)
        : ql_statement_t(statement_type::drop_database, database, collection_name_t()) {
    }

} // namespace components::protocol
