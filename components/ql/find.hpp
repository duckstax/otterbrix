#include "expr.hpp"
#include "ql_statement.hpp"

namespace components::ql {

    struct find_statement final : public ql_statement_t {
        find_statement(database_name_t database, collection_name_t collection, std::vector<expr_ptr> conditions);
        std::vector<expr_ptr> conditions_;
    };

    struct find_one_statement final : public ql_statement_t {
        find_one_statement(database_name_t database, collection_name_t collection, std::vector<expr_ptr> conditions);
        std::vector<expr_ptr> conditions_;
    };

} // namespace components::ql