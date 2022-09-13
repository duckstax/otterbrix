#include "expr.hpp"
#include "ql_statement.hpp"

namespace components::ql {

    struct find_statement final : public ql_statement_t {
        find_statement(database_name_t database, collection_name_t collection, expr_ptr &&condition, bool is_find_one);
        expr_ptr condition_;
    };

} // namespace components::ql