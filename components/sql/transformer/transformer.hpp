#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/nodes/parsenodes.h>

namespace components::sql::transform {
    class transformer {
    public:
        transformer(std::pmr::memory_resource* resource)
            : resource(resource) {}

        logical_plan::node_ptr transform(Node& node, logical_plan::parameter_node_t* statement);

        logical_plan::node_ptr transform_create_database(CreatedbStmt& node);
        logical_plan::node_ptr transform_drop_database(DropdbStmt& node);
        logical_plan::node_ptr transform_create_table(CreateStmt& node);
        logical_plan::node_ptr transform_drop(DropStmt& node);
        logical_plan::node_ptr transform_select(SelectStmt& node, logical_plan::parameter_node_t* statement);
        logical_plan::node_ptr transform_update(UpdateStmt& node, logical_plan::parameter_node_t* statement);
        logical_plan::node_ptr transform_insert(InsertStmt& node);
        logical_plan::node_ptr transform_delete(DeleteStmt& node, logical_plan::parameter_node_t* statement);
        logical_plan::node_ptr transform_create_index(IndexStmt& node);

    private:
        std::pmr::memory_resource* resource;
    };
} // namespace components::sql::transform