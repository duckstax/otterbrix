#include "sql/transformer/transformer.hpp"
#include "sql/transformer/utils.hpp"
#include <logical_plan/node_create_index.hpp>

using namespace components::expressions;

namespace components::sql::transform {
    logical_plan::node_ptr transformer::transform_create_index(IndexStmt& node) {
        assert(node.relation->relname && node.relation->schemaname);

        auto create_index = logical_plan::make_node_create_index(resource,
                                                                 {node.relation->schemaname, node.relation->relname},
                                                                 node.idxname);
        for (auto key : node.indexParams->lst) {
            create_index->keys().emplace_back(pg_ptr_cast<IndexElem>(key.data)->name);
        }
        return create_index;
    }

} // namespace components::sql::transform
