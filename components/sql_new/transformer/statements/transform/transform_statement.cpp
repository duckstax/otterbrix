#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"

namespace components::sql_new::transform {
    std::unique_ptr<statements::parsed_statement> transformer::transform_statement(Node& stmt) {
        switch (stmt.type) {
            case T_SelectStmt:
                return transform_select_statement(pg_cast<SelectStmt>(stmt));
            default:
                throw std::runtime_error("Unimplemented statement type " + node_tag_to_string(stmt.type));
        }
    }
} // namespace components::sql_new::transform
