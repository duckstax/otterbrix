#pragma once

#include <components/expressions/expression.hpp>
#include <components/ql/aggregate.hpp>
#include <components/ql/ql_param_statement.hpp>
#include "join_types.h"

namespace components::ql {

    struct join_t final : ql_param_statement_t {
        join_type join {join_type::inner};
        aggregate_statement left;
        aggregate_statement right;
        std::vector<expressions::expression_ptr> expressions;

        join_t();
        join_t(database_name_t database, collection_name_t collection);
        join_t(database_name_t database, collection_name_t collection, join_type join);
    };

    template <class OStream>
    OStream &operator<<(OStream &stream, const join_t &join) {
        stream << "$join: {";
        stream << "$type: " << magic_enum::enum_name(join.join);
        for (const auto& expr : join.expressions) {
            stream << ", " << expr->to_string();
        }
        stream << ", " << join.left;
        stream << ", " << join.right;
        stream << "}";
        return stream;
    }

} // namespace components::ql
