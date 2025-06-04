#include "transfrom_common.hpp"
#include <components/expressions/aggregate_expression.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <sql/parser/pg_functions.h>

using namespace components::expressions;

namespace components::sql::transform {
    logical_plan::node_ptr transformer::transform_insert(InsertStmt& node, logical_plan::parameter_node_t* params) {
        auto fields = pg_ptr_cast<List>(node.cols)->lst;
        std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>> key_translation(resource);
        for (const auto& field : fields) {
            auto name = pg_ptr_cast<ResTarget>(field.data)->name;
            key_translation.emplace_back(name, name);
        }

        if (pg_ptr_cast<SelectStmt>(node.selectStmt)->valuesLists) {
            auto vals = pg_ptr_cast<List>(pg_ptr_cast<SelectStmt>(node.selectStmt)->valuesLists)->lst;

            std::pmr::vector<components::document::document_ptr> documents(resource);

            for (auto row : vals) {
                auto values = pg_ptr_cast<List>(row.data)->lst;
                assert(values.size() == fields.size());

                auto doc = document::make_document(resource);
                auto it_field = key_translation.begin();
                for (auto it_value = values.begin(); it_value != values.end(); ++it_field, ++it_value) {
                    auto tape = std::make_unique<document::impl::base_document>(resource);
                    doc->set(it_field->first.as_string(),
                             impl::get_value(pg_ptr_cast<Node>(it_value->data), tape.get()).first);
                }
                documents.push_back(doc);
            }

            return logical_plan::make_node_insert(resource,
                                                  rangevar_to_collection(node.relation),
                                                  std::move(documents),
                                                  std::move(key_translation));
        } else {
            auto res = logical_plan::make_node_insert(resource, rangevar_to_collection(node.relation));
            res->append_child(transform_select(*pg_ptr_cast<SelectStmt>(node.selectStmt), params));
            for (const auto& aggregate_child : res->children().back()->children()) {
                if (aggregate_child->type() != logical_plan::node_type::group_t) {
                    continue;
                }

                std::pmr::vector<expressions::key_t> select_fields;
                select_fields.reserve(key_translation.size());
                for (const auto& expr : aggregate_child->expressions()) {
                    if (expr->group() == expression_group::scalar) {
                        select_fields.emplace_back(reinterpret_cast<const scalar_expression_ptr&>(expr)->key());
                    }
                }
                if (key_translation.size() != select_fields.size()) {
                    throw parser_exception_t("Insert column count has to be equal to Select column count",
                                             "Or use SELSECT * FROM");
                }
                assert(key_translation.size() == select_fields.size());
                for (size_t i = 0; i < key_translation.size(); i++) {
                    key_translation[i].second = select_fields[i];
                }
            }
            res->key_translation() = key_translation;
            return res;
        }
    }
} // namespace components::sql::transform
