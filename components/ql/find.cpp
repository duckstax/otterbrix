#include "find.hpp"

namespace components::ql {

    find_statement::find_statement(database_name_t database, collection_name_t collection, expressions::compare_expression_ptr &&condition, bool is_find_one)
        : ql_statement_t(is_find_one ? statement_type::find_one : statement_type::find, std::move(database), std::move(collection))
        , condition_(std::move(condition))
    {}

    find_statement_ptr make_find_statement(database_name_t database, collection_name_t collection, expressions::compare_expression_ptr &&condition, bool is_find_one) {
        return new find_statement(std::move(database), std::move(collection), std::move(condition), is_find_one);
    }

} // namespace components::ql