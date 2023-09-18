#pragma once

#include <components/expressions/expression.hpp>
#include <components/ql/aggregate.hpp>
#include <components/ql/ql_param_statement.hpp>
#include "join_types.h"

namespace components::ql {

    struct join_t final : ql_param_statement_t {
        join_type join {join_type::inner};
        ql_statement_ptr left {nullptr};
        ql_statement_ptr right{nullptr};
        std::vector<expressions::expression_ptr> expressions;

        join_t();
        join_t(database_name_t database, collection_name_t collection);
        join_t(database_name_t database, collection_name_t collection, join_type join);
    };

    template <class OStream>
    OStream &operator<<(OStream &stream, const join_t &join) {
        auto out_ql = [&stream](const ql_statement_ptr &ql) {
            if (ql->type() == statement_type::aggregate) {
                stream << *static_cast<aggregate_statement*>(ql.get());
            } else if (ql->type() == statement_type::join) {
                stream << *static_cast<join_t*>(ql.get());
            }
        };

        stream << "$join: {";
        stream << "$type: " << magic_enum::enum_name(join.join);
        stream << ", ";
        out_ql(join.left);
        stream << ", ";
        out_ql(join.right);
        for (const auto& expr : join.expressions) {
            stream << ", " << expr->to_string();
        }
        stream << "}";
        return stream;
    }


    using join_ptr = boost::intrusive_ptr<join_t>;

    join_ptr make_join(join_type join);

} // namespace components::ql
