#include "find.hpp"

namespace components::ql {

    find_statement::find_statement(database_name_t database, collection_name_t collection, expr_ptr &&condition, bool is_find_one)
        : ql_statement_t(is_find_one ? statement_type::find_one : statement_type::find, std::move(database), std::move(collection))
        , condition_(std::move(condition))
    {}

} // namespace components::ql