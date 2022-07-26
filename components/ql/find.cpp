#include "find.hpp"

namespace components::ql {

    find_statement::find_statement(database_name_t database, collection_name_t collection, std::vector<expr_ptr> conditions)
        : ql_statement_t(statement_type::find, std::move(database), std::move(collection))
        , conditions_(std::move(conditions))
    {}

    find_one_statement::find_one_statement(database_name_t database, collection_name_t collection, std::vector<expr_ptr> conditions)
        : ql_statement_t(statement_type::find, std::move(database), std::move(collection))
        , conditions_(std::move(conditions))
    {}

} // namespace components::ql