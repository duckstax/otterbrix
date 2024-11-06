#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"

namespace components::sql_new::transform {
    std::string transformer::transform_collation(CollateClause* collate) {
        if (!collate) {
            return {};
        }

        std::string collation;
        for (const auto& c : collate->collname->lst) {
            auto pgvalue = pg_ptr_cast<Value>(c.data);
            if (pgvalue->type != T_String) {
                throw std::runtime_error("Expected a string as collation type!");
            }

            auto collation_argument = std::string(pgvalue->val.str);
            if (collation.empty()) {
                collation = collation_argument;
            } else {
                collation += "." + collation_argument;
            }
        }
        return collation;
    }
} // namespace components::sql_new::transform
