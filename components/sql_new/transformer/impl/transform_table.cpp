#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"
#include <logical_plan/node_create_collection.hpp>
#include <logical_plan/node_drop_collection.hpp>
#include <logical_plan/node_drop_index.hpp>

namespace components::sql_new::transform {
    logical_plan::node_ptr transformer::transform_create_table(CreateStmt& node) {
        return new logical_plan::node_create_collection_t(resource, rangevar_to_collection(node.relation));
    }

    logical_plan::node_ptr transformer::transform_drop(DropStmt& node) {
        switch (node.removeType) {
            case OBJECT_TABLE: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                if (drop_name.size() == 1) {
                    return new logical_plan::node_drop_collection_t(
                        resource,
                        {database_name_t(), strVal(drop_name.front().data)});
                }

                auto it = drop_name.begin();
                return new logical_plan::node_drop_collection_t(resource, {strVal((it++)->data), strVal(it->data)});
            }
            case OBJECT_INDEX: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                assert(drop_name.size() == 3);

                auto it = drop_name.begin();
                auto* ql =
                    new components::ql::drop_index_t{strVal((it++)->data), strVal((it++)->data), strVal(it->data)};
                return new logical_plan::node_drop_index_t{resource, {ql->database_, ql->collection_}, ql};
            }
            default:
                throw std::runtime_error("Unsupported removeType");
        }
    }

} // namespace components::sql_new::transform
