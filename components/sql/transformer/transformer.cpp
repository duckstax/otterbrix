#include "transformer.hpp"
#include "utils.hpp"

namespace components::sql::transform {

    logical_plan::node_ptr transformer::transform(Node& node, logical_plan::parameter_node_t* statement) {
        // TODO: Error handling
        switch (node.type) {
            case T_CreatedbStmt:
                return transform_create_database(pg_cast<CreatedbStmt>(node));
            case T_DropdbStmt:
                return transform_drop_database(pg_cast<DropdbStmt>(node));
            case T_CreateStmt:
                return transform_create_table(pg_cast<CreateStmt>(node));
            case T_DropStmt:
                return transform_drop(pg_cast<DropStmt>(node));
            case T_SelectStmt:
                return transform_select(pg_cast<SelectStmt>(node), statement);
            case T_UpdateStmt:
                return transform_update(pg_cast<UpdateStmt>(node), statement);
            case T_InsertStmt:
                return transform_insert(pg_cast<InsertStmt>(node));
            case T_DeleteStmt:
                return transform_delete(pg_cast<DeleteStmt>(node), statement);
            case T_IndexStmt:
                return transform_create_index(pg_cast<IndexStmt>(node));
            default:
                throw std::runtime_error("Unsupported node type: " + node_tag_to_string(node.type));
        }
    }
} // namespace components::sql::transform
