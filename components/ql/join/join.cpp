#include "join.hpp"

namespace components::ql {

    join_t::join_t()
        : ql_param_statement_t(statement_type::join, {}, {})
        , join(join_type::inner)
        , left({}, {})
        , right({}, {}) {
    }

    join_t::join_t(database_name_t database, collection_name_t collection)
        : ql_param_statement_t(statement_type::join, database, collection)
        , join(join_type::inner)
        , left(database, collection)
        , right(database, collection) {
    }

    join_t::join_t(database_name_t database, collection_name_t collection, join_type join)
        : ql_param_statement_t(statement_type::join, database, collection)
        , join(join)
        , left(database, collection)
        , right(database, collection) {
    }

} // namespace components::ql
