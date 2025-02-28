#pragma once

#include "sql_new/parser/nodes/parsenodes.h"
#include <document/impl/document.hpp>
#include <logical_plan/node.hpp>

namespace components::sql_new::transform {
    class transformer {
    public:
        transformer(std::pmr::memory_resource* resource)
            : resource(resource) {}

        logical_plan::node_ptr transform(Node& node, document::impl::base_document* tape);

        logical_plan::node_ptr transform_create_database(CreatedbStmt& node);
        logical_plan::node_ptr transform_drop_database(DropdbStmt& node);
        logical_plan::node_ptr transform_create_table(CreateStmt& node);
        logical_plan::node_ptr transform_drop(DropStmt& node);
        logical_plan::node_ptr transform_select(SelectStmt& node, document::impl::base_document* tape);
        logical_plan::node_ptr transform_update(UpdateStmt& node);
        logical_plan::node_ptr transform_insert(InsertStmt& node);
        logical_plan::node_ptr transform_delete(DeleteStmt& node);
        logical_plan::node_ptr transform_create_index(IndexStmt& node);

    private:
        std::pmr::memory_resource* resource;
    };
} // namespace components::sql_new::transform