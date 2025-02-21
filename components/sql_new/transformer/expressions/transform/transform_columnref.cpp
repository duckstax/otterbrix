#include "sql_new/transformer/expressions/columnref_expression.hpp"
#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"

namespace components::sql_new::transform {
    std::unique_ptr<expressions::parsed_expression> transformer::transform_columnref(ColumnRef& node) {
        auto fields = node.fields;
        auto head = pg_ptr_cast<Node>(fields->lst.front().data);

        switch (head->type) {
            case T_String: {
                if (fields->lst.empty()) {
                    throw std::runtime_error("Unexpected field length");
                }

                std::vector<std::string> column_names;
                for (const auto& cell : fields->lst) {
                    column_names.emplace_back(pg_ptr_cast<Value>(cell.data)->val.str);
                }

                auto colref = std::make_unique<expressions::columnref_expression>(std::move(column_names));
                colref->location = node.location;
                return std::move(colref);
            }
            case T_A_Star:
                return std::make_unique<expressions::columnref_expression>(true);
            default:
                throw std::runtime_error("ColumnRef is not implemented!");
        }
    }
} // namespace components::sql_new::transform
