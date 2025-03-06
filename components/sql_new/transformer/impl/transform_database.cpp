#include "sql_new/transformer/transformer.hpp"
#include <logical_plan/node_create_database.hpp>
#include <logical_plan/node_drop_database.hpp>

namespace components::sql_new::transform {
    logical_plan::node_ptr transformer::transform_create_database(CreatedbStmt& node) {
        return new logical_plan::node_create_database_t{resource, {node.dbname, collection_name_t()}};
    }

    logical_plan::node_ptr transformer::transform_drop_database(DropdbStmt& node) {
        return new logical_plan::node_drop_database_t{resource, {node.dbname, collection_name_t()}};
    }

} // namespace components::sql_new::transform
