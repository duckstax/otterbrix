#pragma once

#include "ql_statement.hpp"

namespace components::ql {

    enum class aggregate_t : char {
        match = 0,
        group,
        project,
        sort,
        limit,
        sum,
        lookup,
        out,
        merge,
        unionWith
    };

    class math_t {};
    class group_t {};
    class project_t {};
    class sort_t {};
    class limit_t {};
    class sum_t {};
    class lookup_t {};
    class out_t {};
    class merge_t {};
    class unionWith_t {};

    class aggregate_statement : public ql_statement_t {
    public:
        aggregate_statement(database_name_t database, collection_name_t collection, expr_ptr&& condition)
            : ql_statement_t(statement_type::aggregate, std::move(database), std::move(collection))
            , condition_(std::move(condition)) {}

        math_t* math_;
        group_t* group_;
        project_t* project_;
        sort_t* sort_;
        limit_t* limit_;
        sum_t* sum_;
        lookup_t* lookup_;
        out_t* out_;
        merge_t* merge_;
        unionWith_t* unionWith_;
    };

} // namespace components::ql