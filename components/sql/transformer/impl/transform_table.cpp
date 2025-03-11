#include "sql/transformer/transformer.hpp"
#include "sql/transformer/utils.hpp"
#include <logical_plan/node_create_collection.hpp>
#include <logical_plan/node_drop_collection.hpp>
#include <logical_plan/node_drop_index.hpp>

namespace components::sql::transform {
    logical_plan::node_ptr transformer::transform_create_table(CreateStmt& node) {
        return logical_plan::make_node_create_collection(resource, rangevar_to_collection(node.relation));
    }

    logical_plan::node_ptr transformer::transform_drop(DropStmt& node) {
        switch (node.removeType) {
            case OBJECT_TABLE: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                if (drop_name.size() == 1) {
                    return logical_plan::make_node_drop_collection(resource,
                                                                   {database_name_t(), strVal(drop_name.front().data)});
                }

                auto it = drop_name.begin();
                return logical_plan::make_node_drop_collection(resource, {strVal((it++)->data), strVal(it->data)});
            }
            case OBJECT_INDEX: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                assert(drop_name.size() == 3);

                auto it = drop_name.begin();
                return logical_plan::make_node_drop_index(resource,
                                                          {strVal((it++)->data), strVal((it++)->data)},
                                                          strVal(it->data));
            }
            default:
                throw std::runtime_error("Unsupported removeType");
        }
    }

} // namespace components::sql::transform
