#include "find.hpp"

using ::document::impl::array_t;
using ::document::impl::value_t;
using ::document::impl::value_type;

namespace components::ql {

    find_statement::find_statement(database_name_t& database, collection_name_t& collection)
        : ql_statement_t(statement_type::find,database,collection){}

    find_one_statement::find_one_statement(database_name_t& database, collection_name_t& collection)
        : ql_statement_t(statement_type::find_one,database,collection){}

} // namespace components::ql