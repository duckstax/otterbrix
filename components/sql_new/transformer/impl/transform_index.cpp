#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"
#include <expressions/aggregate_expression.hpp>
#include <logical_plan/node_create_index.hpp>
#include <ql/index.hpp>

using namespace components::expressions;

namespace components::sql_new::transform {
    logical_plan::node_ptr transformer::transform_create_index(IndexStmt& node) {
        assert(node.relation->relname && node.relation->schemaname);

        auto* create_index =
            new components::ql::create_index_t(node.relation->schemaname, node.relation->relname, node.idxname);
        for (auto key : node.indexParams->lst) {
            create_index->keys_.emplace_back(pg_ptr_cast<IndexElem>(key.data)->name);
        }

        return new logical_plan::node_create_index_t{resource, rangevar_to_collection(node.relation), create_index};
    }

} // namespace components::sql_new::transform
