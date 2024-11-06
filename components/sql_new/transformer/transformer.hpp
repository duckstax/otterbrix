#pragma once

#include "case_insensitive_map.hpp"
#include "sql_new/parser/nodes/nodes.h"
#include "sql_new/parser/nodes/parsenodes.h"
#include "statements/statements.hpp"
#include "tokens.hpp"
#include <memory>

namespace components::sql_new::transform {
    class transformer {
    public:
        const transformer& get_root() const;
        transformer& get_root();

        void set_param(size_t key, size_t value);
        bool get_param(size_t key, size_t& value);

        size_t get_param_count() const;
        void set_param_count(size_t new_count);

        std::unique_ptr<expressions::parsed_expression> transform_expression(Node& node);
        std::unique_ptr<expressions::parsed_expression> transform_expression(Node* node);

        std::unique_ptr<expressions::parsed_expression> transform_columnref(ColumnRef& node);
        std::unique_ptr<expressions::parsed_expression> transform_a_const(A_Const& node);
        std::unique_ptr<expressions::value_expression> transform_value(Value val);
        //        TransformAExpr(pg_cast<A_Expr>(node));
        //        TransformFuncCall(pg_cast<FuncCall>(node));
        std::unique_ptr<expressions::parsed_expression> transform_bool_expr(BoolExpr& node);
        //        TransformTypeCast(pg_cast<TypeCast>(node));
        std::unique_ptr<expressions::parsed_expression> transform_case_expr(CaseExpr& node);
        //        TransformSubquery(pg_cast<SubLink>(node));
        std::unique_ptr<expressions::parsed_expression> transform_coalesce(A_Expr& node);
        std::unique_ptr<expressions::parsed_expression> transform_null_test(NullTest& node);
        std::unique_ptr<expressions::parsed_expression> transform_res_target(ResTarget& node);
        std::unique_ptr<expressions::parsed_expression> transform_paramref(ParamRef& node);
        std::unique_ptr<expressions::parsed_expression> transform_named_arg(NamedArgExpr& node);
        std::unique_ptr<expressions::parsed_expression> transform_collate_clause(CollateClause& node);
        std::unique_ptr<expressions::parsed_expression> transform_array_indirection(A_Indirection& indirection_node);
        std::unique_ptr<expressions::parsed_expression> transform_grouping_func(GroupingFunc& grouping);
        std::unique_ptr<expressions::parsed_expression> transform_boolean_test(BooleanTest& node);

        std::unique_ptr<statements::parsed_statement> transform_statement(Node &stmt);
        std::unique_ptr<statements::parsed_statement> transform_select_statement(SelectStmt &stmt);

        std::string transform_collation(CollateClause* collate);
    private:
        transformer* parent = nullptr;
        //! Parser options
        //        ParserOptions &options;
        //! The current prepared statement parameter index
        size_t prepared_statement_parameter_index = 0;
        //! Map from named parameter to parameter index;
        std::unordered_map<size_t, size_t> named_param_map;
        //! Holds window expressions defined by name. We need those when transforming the expressions referring to them.
        case_insensitive_map_t<WindowDef*> window_clauses;
        //! The set of pivot entries to create
        //        vector<unique_ptr<CreatePivotEntry>> pivot_entries;
        //! Sets of stored CTEs, if any
        //        vector<CommonTableExpressionMap *> stored_cte_map;
        //! Whether or not we are currently binding a window definition
        bool in_window_definition = false;
    };
} // namespace components::sql_new::transform