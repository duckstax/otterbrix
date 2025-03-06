#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"
#include "transfrom_common.hpp"
#include <expressions/aggregate_expression.hpp>
#include <logical_plan/node_insert.hpp>

using namespace components::expressions;

namespace components::sql_new::transform {
    logical_plan::node_ptr transformer::transform_insert(InsertStmt& node) {
        auto fields = pg_ptr_cast<List>(node.cols)->lst;
        auto vals = pg_ptr_cast<List>(pg_ptr_cast<SelectStmt>(node.selectStmt)->valuesLists)->lst;

        std::pmr::vector<components::document::document_ptr> documents(resource);
        for (auto row : vals) {
            auto values = pg_ptr_cast<List>(row.data)->lst;
            assert(values.size() == fields.size());

            auto doc = document::make_document(resource);
            auto it_field = fields.begin();
            for (auto it_value = values.begin(); it_value != values.end(); ++it_field, ++it_value) {
                auto tape = std::make_unique<document::impl::base_document>(resource);
                doc->set(pg_ptr_cast<ResTarget>(it_field->data)->name,
                         impl::get_value(pg_ptr_cast<Node>(it_value->data), tape.get()).first);
            }
            documents.push_back(doc);
        }

        return new logical_plan::node_insert_t{resource, rangevar_to_collection(node.relation), std::move(documents)};
    }
} // namespace components::sql_new::transform
