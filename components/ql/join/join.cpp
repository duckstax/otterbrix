#include "join.hpp"

namespace components::ql {

    join_t::join_t(database_name_t database, collection_name_t collection, join_type join)
        : ql_statement_t(statement_type::join, database, collection)
        , join(join) {
    }

} // namespace components::ql
