/*-------------------------------------------------------------------------
*
* primnodes.h
*	  Definitions for "primitive" node types, those that are used in more
*	  than one of the parse/plan/execute stages of the query pipeline.
*	  Currently, these are mostly nodes for executable expressions
*	  and join trees.
*
*
* Portions Copyright (c) 2005-2009, Greenplum inc
* Portions Copyright (c) 2012-Present Pivotal Software, Inc.
* Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
* Portions Copyright (c) 1994, Regents of the University of California
*
* src/include/nodes/primnodes.h
*
*-------------------------------------------------------------------------
*/
#ifndef PRIMNODES_H
#define PRIMNODES_H

#include "attrnum.h"
#include "nodes.h"
#include "pg_type_definitions.h"
#include <components/sql/parser/pg_std_list.h>

/* ----------------------------------------------------------------
*						node definitions
* ----------------------------------------------------------------
*/

/*
* Alias -
*	  specifies an alias for a range variable; the alias might also
*	  specify renaming of columns within the table.
*
* Note: colnames is a list of Value nodes (always strings).  In Alias structs
* associated with RTEs, there may be entries corresponding to dropped
* columns; these are normally empty strings ("").  See parsenodes.h for info.
*/
typedef struct Alias {
    NodeTag type;
    char* aliasname; /* aliased rel name (never qualified) */
    List* colnames;  /* optional list of column aliases */
} Alias;

typedef enum InhOption
{
    INH_NO,     /* Do NOT scan child tables */
    INH_YES,    /* DO scan child tables */
    INH_DEFAULT /* Use current SQL_inheritance option */
} InhOption;

/* What to do at commit time for temporary relations */
typedef enum OnCommitAction
{
    ONCOMMIT_NOOP,          /* No ON COMMIT clause (do nothing) */
    ONCOMMIT_PRESERVE_ROWS, /* ON COMMIT PRESERVE ROWS (do nothing) */
    ONCOMMIT_DELETE_ROWS,   /* ON COMMIT DELETE ROWS */
    ONCOMMIT_DROP           /* ON COMMIT DROP */
} OnCommitAction;

/*
* RangeVar - range variable, used in FROM clauses
*
* Also used to represent table names in utility statements; there, the alias
* field is not used, and inhOpt shows whether to apply the operation
* recursively to child tables.  In some contexts it is also useful to carry
* a TEMP table indication here.
*/
typedef struct RangeVar {
    NodeTag type;
    char* uid;           /* the unique identifier, or NULL */
    char* catalogname;   /* the catalog (database) name, or NULL */
    char* schemaname;    /* the schema name, or NULL */
    char* relname;       /* the relation/sequence name */
    InhOption inhOpt;    /* expand rel by inheritance? recursively act
                                * on children? */
    char relpersistence; /* see RELPERSISTENCE_* in pg_class.h */
    Alias* alias;        /* table alias & optional column aliases */
    int location;        /* token location, or -1 if unknown */
} RangeVar;

/*
* IntoClause - target information for SELECT INTO, CREATE TABLE AS, and
* CREATE MATERIALIZED VIEW
*
* For CREATE MATERIALIZED VIEW, viewQuery is the parsed-but-not-rewritten
* SELECT Query for the view; otherwise it's NULL.  (Although it's actually
* Query*, we declare it as Node* to avoid a forward reference.)
*/
typedef struct IntoClause {
    NodeTag type;

    RangeVar* rel;           /* target relation name */
    List* colNames;          /* column names to assign, or NIL */
    List* options;           /* options from WITH clause */
    OnCommitAction onCommit; /* what do we do at COMMIT? */
    char* tableSpaceName;    /* table space to use, or NULL */
    Node* viewQuery;         /* materialized view's SELECT query */
    bool skipData;           /* true for WITH NO DATA */
    Node* distributedBy;     /* GPDB: columns to distribubte the data on. */
} IntoClause;

typedef struct CopyIntoClause {
    NodeTag type;

    List* attlist;   /* List of column names (as Strings), or NIL
                                * for all columns */
    bool is_program; /* is 'filename' a program to popen? */
    char* filename;  /* filename, or NULL for STDIN/STDOUT */
    List* options;   /* List of DefElem nodes */
    List* ao_segnos; /* AO segno map */
} CopyIntoClause;

typedef struct RefreshClause {
    NodeTag type;

    bool concurrent; /* allow concurrent access? */
    bool skipData;
    RangeVar* relation; /* relation to insert into */
} RefreshClause;

/* ----------------------------------------------------------------
*					node types for executable expressions
* ----------------------------------------------------------------
*/

/*
* Expr - generic superclass for executable-expression nodes
*
* All node types that are used in executable expression trees should derive
* from Expr (that is, have Expr as their first field).  Since Expr only
* contains NodeTag, this is a formality, but it is an easy form of
* documentation.  See also the ExprState node types in execnodes.h.
*/
typedef struct Expr {
    NodeTag type;
} Expr;

/*
* Var - expression node representing a variable (ie, a table column)
*
* Note: during parsing/planning, varnoold/varoattno are always just copies
* of varno/varattno.  At the tail end of planning, Var nodes appearing in
* upper-level plan nodes are reassigned to point to the outputs of their
* subplans; for example, in a join node varno becomes INNER_VAR or OUTER_VAR
* and varattno becomes the index of the proper element of that subplan's
* target list.  But varnoold/varoattno continue to hold the original values.
* The code doesn't really need varnoold/varoattno, but they are very useful
* for debugging and interpreting completed plans, so we keep them around.
*/
#define INNER_VAR 65000 /* reference to inner subplan */
#define OUTER_VAR 65001 /* reference to outer subplan */
#define INDEX_VAR 65002 /* reference to index column */

#define IS_SPECIAL_VARNO(varno) ((varno) >= INNER_VAR)

/* Symbols for the indexes of the special RTE entries in rules */
#define PRS2_OLD_VARNO 1
#define PRS2_NEW_VARNO 2

typedef struct Var {
    Expr xpr;
    Index varno;          /* index of this var's relation in the range
                                * table, or INNER_VAR/OUTER_VAR/INDEX_VAR */
    AttrNumber varattno;  /* attribute number of this var, or zero for
                                * all */
    Oid vartype;          /* pg_type OID for the type of this var */
    int32_t vartypmod;    /* pg_attribute typmod value */
    Oid varcollid;        /* OID of collation, or InvalidOid if none */
    Index varlevelsup;    /* for subquery variables referencing outer
                                * relations; 0 in a normal var, >0 means N
                                * levels up */
    Index varnoold;       /* original value of varno, for debugging */
    AttrNumber varoattno; /* original value of varattno */
    int location;         /* token location, or -1 if unknown */
} Var;

/*
* Const
*/
typedef struct Const {
    Expr xpr;
    Oid consttype;       /* pg_type OID of the constant's datatype */
    int32_t consttypmod; /* typmod value, if any */
    Oid constcollid;     /* OID of collation, or InvalidOid if none */
    int constlen;        /* typlen of the constant's datatype */
    Datum constvalue;    /* the constant's value */
    bool constisnull;    /* whether the constant is null (if true,
                                * constvalue is undefined) */
    bool constbyval;     /* whether this datatype is passed by value.
                                * If true, then all the information is stored
                                * in the Datum. If false, then the Datum
                                * contains a pointer to the information. */
    int location;        /* token location, or -1 if unknown */
} Const;

/* ----------------
* Param
*		paramkind - specifies the kind of parameter. The possible values
*		for this field are:
*
*		PARAM_EXTERN:  The parameter value is supplied from outside the plan.
*				Such parameters are numbered from 1 to n.
*
*		PARAM_EXEC:  The parameter is an internal executor parameter, used
*				for passing values into and out of sub-queries or from
*				nestloop joins to their inner scans.
*				For historical reasons, such parameters are numbered from 0.
*				These numbers are independent of PARAM_EXTERN numbers.
*
*		PARAM_SUBLINK:	The parameter represents an output column of a SubLink
*				node's sub-select.  The column number is contained in the
*				`paramid' field.  (This type of Param is converted to
*				PARAM_EXEC during planning.)
*
* Note: currently, paramtypmod is valid for PARAM_SUBLINK Params, and for
* PARAM_EXEC Params generated from them; it is always -1 for PARAM_EXTERN
* params, since the APIs that supply values for such parameters don't carry
* any typmod info.
* ----------------
*/
typedef enum ParamKind
{
    PARAM_EXTERN,
    PARAM_EXEC,
    PARAM_EXEC_REMOTE, /* MPP ???? */
    PARAM_SUBLINK
} ParamKind;

typedef struct Param {
    Expr xpr;
    ParamKind paramkind; /* kind of parameter. See above */
    int paramid;         /* numeric ID for parameter */
    Oid paramtype;       /* pg_type OID of parameter's datatype */
    int32_t paramtypmod; /* typmod value, if known */
    Oid paramcollid;     /* OID of collation, or InvalidOid if none */
    int location;        /* token location, or -1 if unknown */
} Param;

/* AggStage enumeration indicates how the executor should handle an
* Aggref node.
*/
typedef enum AggStage
{
    AGGSTAGE_NORMAL = 0,
    AGGSTAGE_PARTIAL,      /* First (lower, earlier) stage of 2-stage aggregation. */
    AGGSTAGE_INTERMEDIATE, /* The intermediate stage between AGGSTAGE_PARTIAL and
                           * AGGSTAGE_FINAL that handles the higher aggregation
                           * level in a (partial) ROLLUP grouping extension
                           * query.
                           */
    AGGSTAGE_FINAL         /* Second (upper, later) stage of 2-stage aggregation. */
} AggStage;

/*
* Aggref
*
* The aggregate's args list is a targetlist, ie, a list of TargetEntry nodes.
*
* For a normal (non-ordered-set) aggregate, the non-resjunk TargetEntries
* represent the aggregate's regular arguments (if any) and resjunk TLEs can
* be added at the end to represent ORDER BY expressions that are not also
* arguments.  As in a top-level Query, the TLEs can be marked with
* ressortgroupref indexes to let them be referenced by SortGroupClause
* entries in the aggorder and/or aggdistinct lists.  This represents ORDER BY
* and DISTINCT operations to be applied to the aggregate input rows before
* they are passed to the transition function.  The grammar only allows a
* simple "DISTINCT" specifier for the arguments, but we use the full
* query-level representation to allow more code sharing.
*
* For an ordered-set aggregate, the args list represents the WITHIN GROUP
* (aggregated) arguments, all of which will be listed in the aggorder list.
* DISTINCT is not supported in this case, so aggdistinct will be NIL.
* The direct arguments appear in aggdirectargs (as a list of plain
* expressions, not TargetEntry nodes).
*/
typedef struct Aggref {
    Expr xpr;
    Oid aggfnoid;        /* pg_proc Oid of the aggregate */
    Oid aggtype;         /* type Oid of result of the aggregate */
    Oid aggcollid;       /* OID of collation of result */
    Oid inputcollid;     /* OID of collation that function should use */
    List* aggdirectargs; /* direct arguments, if an ordered-set agg */
    List* args;          /* aggregated arguments and sort expressions */
    List* aggorder;      /* ORDER BY (list of SortGroupClause) */
    List* aggdistinct;   /* DISTINCT (list of SortGroupClause) */
    Expr* aggfilter;     /* FILTER expression, if any */
    bool aggstar;        /* TRUE if argument list was really '*' */
    bool aggvariadic;    /* true if variadic arguments have been
                                * combined into an array last argument */
    char aggkind;        /* aggregate kind (see pg_aggregate.h) */
    Index agglevelsup;   /* > 0 if agg belongs to outer query */
    AggStage aggstage;   /* MPP: 2-stage? If so, which stage */
    int location;        /* token location, or -1 if unknown */
} Aggref;

/*
* Grouping: describe the hidden GROUPING column for grouping extensions.
*
* Defined for making it easily to distinguish this column with others.
*
* Used with GroupingFunc to distinguish 'null' values that are created
* through grouping with those that are in the raw data. See also GroupingFunc
* for more details.
*/
typedef struct Grouping {
    Expr xpr;
} Grouping;

/*
* GroupId -
*    representation of the hidden GROUP_ID column for grouping extensions.
*
* Defined to make it easy to distinguish this column from others.
*
* This is used to determine whether output tuples are coming from
* duplicate grouping sets. For example, a table
*
*    test (a integer, b integer)
*
* has two rows:
*
*       (1,2), (1,2).
*
* Consider a rollup clause "rollup(a),a", which contains a grouping
* set (a) twice. Therefore, the query
*
*    select a,sum(b),group_id() from test group by rollup(a),a;
*
* returns two rows:
*
*    1,4,0
*    1,4,1
*
* The GROUP_ID value 0 indicates this tuple is from the grouping set (a).
* The value 1 indicates this tuple is from the first duplicate grouping set of
* (a).
*
* This query can be also re-written to the following:
*
*    select a,avg(b),0 from test group by a
*      union all
*    select a,avg(b),1 from test group by a;
*/
typedef struct GroupId {
    Expr xpr;
} GroupId;

/*
* WindowFunc: describes a window function call
*
* In a query tree, a WindowFunc corresponds to a SQL window function
* call.  In a plan tree, a WindowRef is an expression the corresponds
* to some or all of the calculation of the window function result.
*
*/
typedef struct WindowFunc {
    Expr xpr;
    Oid winfnoid;     /* pg_proc Oid of the function */
    Oid wintype;      /* type Oid of result of the window function */
    Oid wincollid;    /* OID of collation of result */
    Oid inputcollid;  /* OID of collation that function should use */
    List* args;       /* arguments to the window function */
    Expr* aggfilter;  /* FILTER expression, if any */
    Index winref;     /* index of associated WindowClause */
    bool winstar;     /* TRUE if argument list was really '*' */
    bool winagg;      /* is function a simple aggregate? */
    bool windistinct; /* TRUE if it's agg(DISTINCT ...) */
    int location;     /* token location, or -1 if unknown */
} WindowFunc;

/* ----------------
*	ArrayRef: describes an array subscripting operation
*
* An ArrayRef can describe fetching a single element from an array,
* fetching a subarray (array slice), storing a single element into
* an array, or storing a slice.  The "store" cases work with an
* initial array value and a source value that is inserted into the
* appropriate part of the array; the result of the operation is an
* entire new modified array value.
*
* If reflowerindexpr = NIL, then we are fetching or storing a single array
* element at the subscripts given by refupperindexpr.  Otherwise we are
* fetching or storing an array slice, that is a rectangular subarray
* with lower and upper bounds given by the index expressions.
* reflowerindexpr must be the same length as refupperindexpr when it
* is not NIL.
*
* Note: the result datatype is the element type when fetching a single
* element; but it is the array type when doing subarray fetch or either
* type of store.
* ----------------
*/
typedef struct ArrayRef {
    Expr xpr;
    Oid refarraytype;      /* type of the array proper */
    Oid refelemtype;       /* type of the array elements */
    int32_t reftypmod;     /* typmod of the array (and elements too) */
    Oid refcollid;         /* OID of collation, or InvalidOid if none */
    List* refupperindexpr; /* expressions that evaluate to upper array
                                * indexes */
    List* reflowerindexpr; /* expressions that evaluate to lower array
                                * indexes */
    Expr* refexpr;         /* the expression that evaluates to an array
                                * value */
    Expr* refassgnexpr;    /* expression for the source value, or NULL if
                                * fetch */
} ArrayRef;

/*
* CoercionContext - distinguishes the allowed set of type casts
*
* NB: ordering of the alternatives is significant; later (larger) values
* allow more casts than earlier ones.
*/
typedef enum CoercionContext
{
    COERCION_IMPLICIT,   /* coercion in context of expression */
    COERCION_ASSIGNMENT, /* coercion in context of assignment */
    COERCION_EXPLICIT    /* explicit cast operation */
} CoercionContext;

/*
* CoercionForm - how to display a node that could have come from a cast
*
* NB: equal() ignores CoercionForm fields, therefore this *must* not carry
* any semantically significant information.  We need that behavior so that
* the planner will consider equivalent implicit and explicit casts to be
* equivalent.  In cases where those actually behave differently, the coercion
* function's arguments will be different.
*/
typedef enum CoercionForm
{
    COERCE_EXPLICIT_CALL, /* display as a function call */
    COERCE_EXPLICIT_CAST, /* display as an explicit cast */
    COERCE_IMPLICIT_CAST  /* implicit cast, so hide it */
} CoercionForm;

/*
* FuncExpr - expression node for a function call
*/
typedef struct FuncExpr {
    Expr xpr;
    Oid funcid;              /* PG_PROC OID of the function */
    Oid funcresulttype;      /* PG_TYPE OID of result value */
    bool funcretset;         /* true if function returns set */
    bool funcvariadic;       /* true if variadic arguments have been
                                * combined into an array last argument */
    CoercionForm funcformat; /* how to display this function call */
    Oid funccollid;          /* OID of collation of result */
    Oid inputcollid;         /* OID of collation that function should use */
    List* args;              /* arguments to the function */
    int location;            /* token location, or -1 if unknown */
    bool is_tablefunc;       /* Is a TableFunction reference */
} FuncExpr;

/*
* NamedArgExpr - a named argument of a function
*
* This node type can only appear in the args list of a FuncCall or FuncExpr
* node.  We support pure positional call notation (no named arguments),
* named notation (all arguments are named), and mixed notation (unnamed
* arguments followed by named ones).
*
* Parse analysis sets argnumber to the positional index of the argument,
* but doesn't rearrange the argument list.
*
* The planner will convert argument lists to pure positional notation
* during expression preprocessing, so execution never sees a NamedArgExpr.
*/
typedef struct NamedArgExpr {
    Expr xpr;
    Expr* arg;     /* the argument expression */
    char* name;    /* the name */
    int argnumber; /* argument's number in positional notation */
    int location;  /* argument name location, or -1 if unknown */
} NamedArgExpr;

/*
* OpExpr - expression node for an operator invocation
*
* Semantically, this is essentially the same as a function call.
*
* Note that opfuncid is not necessarily filled in immediately on creation
* of the node.  The planner makes sure it is valid before passing the node
* tree to the executor, but during parsing/planning opfuncid can be 0.
*/
typedef struct OpExpr {
    Expr xpr;
    Oid opno;         /* PG_OPERATOR OID of the operator */
    Oid opfuncid;     /* PG_PROC OID of underlying function */
    Oid opresulttype; /* PG_TYPE OID of result value */
    bool opretset;    /* true if operator returns set */
    Oid opcollid;     /* OID of collation of result */
    Oid inputcollid;  /* OID of collation that operator should use */
    List* args;       /* arguments to the operator (1 or 2) */
    int location;     /* token location, or -1 if unknown */
} OpExpr;

/*
* DistinctExpr - expression node for "x IS DISTINCT FROM y"
*
* Except for the nodetag, this is represented identically to an OpExpr
* referencing the "=" operator for x and y.
* We use "=", not the more obvious "<>", because more datatypes have "="
* than "<>".  This means the executor must invert the operator result.
* Note that the operator function won't be called at all if either input
* is NULL, since then the result can be determined directly.
*/
typedef OpExpr DistinctExpr;

/*
* NullIfExpr - a NULLIF expression
*
* Like DistinctExpr, this is represented the same as an OpExpr referencing
* the "=" operator for x and y.
*/
typedef OpExpr NullIfExpr;

/*
* ScalarArrayOpExpr - expression node for "scalar op ANY/ALL (array)"
*
* The operator must yield boolean.  It is applied to the left operand
* and each element of the righthand array, and the results are combined
* with OR or AND (for ANY or ALL respectively).  The node representation
* is almost the same as for the underlying operator, but we need a useOr
* flag to remember whether it's ANY or ALL, and we don't have to store
* the result type (or the collation) because it must be boolean.
*/
typedef struct ScalarArrayOpExpr {
    Expr xpr;
    Oid opno;        /* PG_OPERATOR OID of the operator */
    Oid opfuncid;    /* PG_PROC OID of underlying function */
    bool useOr;      /* true for ANY, false for ALL */
    Oid inputcollid; /* OID of collation that operator should use */
    List* args;      /* the scalar and array operands */
    int location;    /* token location, or -1 if unknown */
} ScalarArrayOpExpr;

/*
* BoolExpr - expression node for the basic Boolean operators AND, OR, NOT
*
* Notice the arguments are given as a List.  For NOT, of course the list
* must always have exactly one element.  For AND and OR, the executor can
* handle any number of arguments.  The parser generally treats AND and OR
* as binary and so it typically only produces two-element lists, but the
* optimizer will flatten trees of AND and OR nodes to produce longer lists
* when possible.  There are also a few special cases where more arguments
* can appear before optimization.
*/
typedef enum BoolExprType
{
    AND_EXPR,
    OR_EXPR,
    NOT_EXPR
} BoolExprType;

typedef struct BoolExpr {
    Expr xpr;
    BoolExprType boolop;
    List* args;   /* arguments to this expression */
    int location; /* token location, or -1 if unknown */
} BoolExpr;

/*
* TableValueExpr - a "TABLE( <subquery> )" expression indicating a subquery
* expression that is passed as a value to a function.
*
* This is <table value constructor by query> within the SQL Standard
*/
typedef struct TableValueExpr {
    NodeTag type;
    Node* subquery;
    int location;
} TableValueExpr;

/*
* SubLink
*
* A SubLink represents a subselect appearing in an expression, and in some
* cases also the combining operator(s) just above it.  The subLinkType
* indicates the form of the expression represented:
*	EXISTS_SUBLINK		EXISTS(SELECT ...)
*	ALL_SUBLINK			(lefthand) op ALL (SELECT ...)
*	ANY_SUBLINK			(lefthand) op ANY (SELECT ...)
*	ROWCOMPARE_SUBLINK	(lefthand) op (SELECT ...)
*	EXPR_SUBLINK		(SELECT with single targetlist item ...)
*	ARRAY_SUBLINK		ARRAY(SELECT with single targetlist item ...)
*	CTE_SUBLINK			WITH query (never actually part of an expression)
*	INITPLAN_FUNC_SUBLINK	for function run as initplan.
* For query like (create table t as select * from f()), QD is used to run
* CTAS, hence f() could only be run on entryDB(or QEs). But entryDB could not
* do the dispatch work. So if f() contains DDLs, the above query would fail.
* We introduce this new INITPLAN_FUNC_SUBLINK to make f() run as initplan
* and store intermidiate result into tuplestore. CTAS will fetch tuples from
* this tuplestore.
* For ALL, ANY, and ROWCOMPARE, the lefthand is a list of expressions of the
* same length as the subselect's targetlist.  ROWCOMPARE will *always* have
* a list with more than one entry; if the subselect has just one target
* then the parser will create an EXPR_SUBLINK instead (and any operator
* above the subselect will be represented separately).  Note that both
* ROWCOMPARE and EXPR require the subselect to deliver only one row.
* ALL, ANY, and ROWCOMPARE require the combining operators to deliver boolean
* results.  ALL and ANY combine the per-row results using AND and OR
* semantics respectively.
* ARRAY requires just one target column, and creates an array of the target
* column's type using any number of rows resulting from the subselect.
*
* SubLink is classed as an Expr node, but it is not actually executable;
* it must be replaced in the expression tree by a SubPlan node during
* planning.
*
* NOTE: in the raw output of gram.y, testexpr contains just the raw form
* of the lefthand expression (if any), and operName is the String name of
* the combining operator.  Also, subselect is a raw parsetree.  During parse
* analysis, the parser transforms testexpr into a complete boolean expression
* that compares the lefthand value(s) to PARAM_SUBLINK nodes representing the
* output columns of the subselect.  And subselect is transformed to a Query.
* This is the representation seen in saved rules and in the rewriter.
*
* In EXISTS, EXPR, and ARRAY SubLinks, testexpr and operName are unused and
* are always null.
*
* The CTE_SUBLINK case never occurs in actual SubLink nodes, but it is used
* in SubPlans generated for WITH subqueries.
*/
typedef enum SubLinkType
{
    EXISTS_SUBLINK,
    ALL_SUBLINK,
    ANY_SUBLINK,
    ROWCOMPARE_SUBLINK,
    EXPR_SUBLINK,
    ARRAY_SUBLINK,
    CTE_SUBLINK,           /* for SubPlans only */
    INITPLAN_FUNC_SUBLINK, /* for function run as initplan */
    NOT_EXISTS_SUBLINK     /* GPORCA uses NOT_EXIST_SUBLINK to implement correlated left anti semijoin. */
} SubLinkType;

typedef struct SubLink {
    Expr xpr;
    SubLinkType subLinkType; /* see above */
    Node* testexpr;          /* outer-query test for ALL/ANY/ROWCOMPARE */
    List* operName;          /* originally specified operator name */
    Node* subselect;         /* subselect as Query* or parsetree */
    int location;            /* token location, or -1 if unknown */
} SubLink;

/*
* SubPlan - executable expression node for a subplan (sub-SELECT)
*
* The planner replaces SubLink nodes in expression trees with SubPlan
* nodes after it has finished planning the subquery.  SubPlan references
* a sub-plantree stored in the subplans list of the toplevel PlannedStmt.
* (We avoid a direct link to make it easier to copy expression trees
* without causing multiple processing of the subplan.)
*
* In an ordinary subplan, testexpr points to an executable expression
* (OpExpr, an AND/OR tree of OpExprs, or RowCompareExpr) for the combining
* operator(s); the left-hand arguments are the original lefthand expressions,
* and the right-hand arguments are PARAM_EXEC Param nodes representing the
* outputs of the sub-select.  (NOTE: runtime coercion functions may be
* inserted as well.)  This is just the same expression tree as testexpr in
* the original SubLink node, but the PARAM_SUBLINK nodes are replaced by
* suitably numbered PARAM_EXEC nodes.
*
* If the sub-select becomes an initplan rather than a subplan, the executable
* expression is part of the outer plan's expression tree (and the SubPlan
* node itself is not, but rather is found in the outer plan's initPlan
* list).  In this case testexpr is NULL to avoid duplication.
*
* The planner also derives lists of the values that need to be passed into
* and out of the subplan.  Input values are represented as a list "args" of
* expressions to be evaluated in the outer-query context (currently these
* args are always just Vars, but in principle they could be any expression).
* The values are assigned to the global PARAM_EXEC params indexed by parParam
* (the parParam and args lists must have the same ordering).  setParam is a
* list of the PARAM_EXEC params that are computed by the sub-select, if it
* is an initplan; they are listed in order by sub-select output column
* position.  (parParam and setParam are integer Lists, not Bitmapsets,
* because their ordering is significant.)
*
* Also, the planner computes startup and per-call costs for use of the
* SubPlan.  Note that these include the cost of the subquery proper,
* evaluation of the testexpr if any, and any hashtable management overhead.
*/
typedef struct SubPlan {
    Expr xpr;
    /* Fields copied from original SubLink: */
    SubLinkType subLinkType; /* see above */
    /* The combining operators, transformed to an executable expression: */
    Node* testexpr; /* OpExpr or RowCompareExpr expression tree */
    List* paramIds; /* IDs of Params embedded in the above */

    int qDispSliceId; /* CDB: slice# of initplan's root slice, or 0 */

    /* Identification of the Plan tree to use: */
    int plan_id; /* Index (from 1) in PlannedStmt.subplans */
    /* Identification of the SubPlan for EXPLAIN and debugging purposes: */
    char* plan_name; /* A name assigned during planning */
    /* Extra data useful for determining subplan's output type: */
    Oid firstColType;       /* Type of first column of subplan result */
    int32_t firstColTypmod; /* Typmod of first column of subplan result */
    Oid firstColCollation;  /* Collation of first column of
                                        * subplan result */
    /* Information about execution strategy: */
    bool useHashTable;    /* TRUE to store subselect output in a hash
                                * table (implies we are doing "IN") */
    bool unknownEqFalse;  /* TRUE if it's okay to return FALSE when the
                                * spec result is UNKNOWN; this allows much
                                * simpler handling of null values */
    bool is_initplan;     /* CDB: Is the subplan implemented as an
                                * initplan? */
    bool is_multirow;     /* CDB: May the subplan return more than
                                * one row? */
    bool is_parallelized; /* Has subplan been processed to be executed in parallel setting */
    /* Information for passing params into and out of the subselect: */
    /* setParam and parParam are lists of integers (param IDs) */
    List* setParam; /* initplan subqueries have to set these
                                * Params for parent plan */
    List* parParam; /* indices of input Params from parent plan */
    List* args;     /* exprs to pass as parParam values */
    List* extParam; /* indices of input Params from ancestor plan */
    /* Estimated execution costs: */
    Cost startup_cost;     /* one-time setup cost */
    Cost per_call_cost;    /* cost for each subplan evaluation */
    bool initPlanParallel; /* CDB: Init plan is parallelled */
} SubPlan;

/*
* AlternativeSubPlan - expression node for a choice among SubPlans
*
* The subplans are given as a List so that the node definition need not
* change if there's ever more than two alternatives.  For the moment,
* though, there are always exactly two; and the first one is the fast-start
* plan.
*/
typedef struct AlternativeSubPlan {
    Expr xpr;
    List* subplans; /* SubPlan(s) with equivalent results */
} AlternativeSubPlan;

/* ----------------
* FieldSelect
*
* FieldSelect represents the operation of extracting one field from a tuple
* value.  At runtime, the input expression is expected to yield a rowtype
* Datum.  The specified field number is extracted and returned as a Datum.
* ----------------
*/

typedef struct FieldSelect {
    Expr xpr;
    Expr* arg;            /* input expression */
    AttrNumber fieldnum;  /* attribute number of field to extract */
    Oid resulttype;       /* type of the field (result type of this
                                * node) */
    int32_t resulttypmod; /* output typmod (usually -1) */
    Oid resultcollid;     /* OID of collation of the field */
} FieldSelect;

/* ----------------
* FieldStore
*
* FieldStore represents the operation of modifying one field in a tuple
* value, yielding a new tuple value (the input is not touched!).  Like
* the assign case of ArrayRef, this is used to implement UPDATE of a
* portion of a column.
*
* A single FieldStore can actually represent updates of several different
* fields.  The parser only generates FieldStores with single-element lists,
* but the planner will collapse multiple updates of the same base column
* into one FieldStore.
* ----------------
*/

typedef struct FieldStore {
    Expr xpr;
    Expr* arg;       /* input tuple value */
    List* newvals;   /* new value(s) for field(s) */
    List* fieldnums; /* integer list of field attnums */
    Oid resulttype;  /* type of result (same as type of arg) */
                     /* Like RowExpr, we deliberately omit a typmod and collation here */
} FieldStore;

/* ----------------
* RelabelType
*
* RelabelType represents a "dummy" type coercion between two binary-
* compatible datatypes, such as reinterpreting the result of an OID
* expression as an int4.  It is a no-op at runtime; we only need it
* to provide a place to store the correct type to be attributed to
* the expression result during type resolution.  (We can't get away
* with just overwriting the type field of the input expression node,
* so we need a separate node to show the coercion's result type.)
* ----------------
*/

typedef struct RelabelType {
    Expr xpr;
    Expr* arg;                  /* input expression */
    Oid resulttype;             /* output type of coercion expression */
    int32_t resulttypmod;       /* output typmod (usually -1) */
    Oid resultcollid;           /* OID of collation, or InvalidOid if none */
    CoercionForm relabelformat; /* how to display this node */
    int location;               /* token location, or -1 if unknown */
} RelabelType;

/* ----------------
* CoerceViaIO
*
* CoerceViaIO represents a type coercion between two types whose textual
* representations are compatible, implemented by invoking the source type's
* typoutput function then the destination type'sfunc typinput function.
* ----------------
*/

typedef struct CoerceViaIO {
    Expr xpr;
    Expr* arg;      /* input expression */
    Oid resulttype; /* output type of coercion */
    /* output typmod is not stored, but is presumed -1 */
    Oid resultcollid;          /* OID of collation, or InvalidOid if none */
    CoercionForm coerceformat; /* how to display this node */
    int location;              /* token location, or -1 if unknown */
} CoerceViaIO;

/* ----------------
* ArrayCoerceExpr
*
* ArrayCoerceExpr represents a type coercion from one array type to another,
* which is implemented by applying the indicated element-type coercion
* function to each element of the source array.  If elemfuncid is InvalidOid
* then the element types are binary-compatible, but the coercion still
* requires some effort (we have to fix the element type ID stored in the
* array header).
* ----------------
*/

typedef struct ArrayCoerceExpr {
    Expr xpr;
    Expr* arg;                 /* input expression (yields an array) */
    Oid elemfuncid;            /* OID of element coercion function, or 0 */
    Oid resulttype;            /* output type of coercion (an array type) */
    int32_t resulttypmod;      /* output typmod (also element typmod) */
    Oid resultcollid;          /* OID of collation, or InvalidOid if none */
    bool isExplicit;           /* conversion semantics flag to pass to func */
    CoercionForm coerceformat; /* how to display this node */
    int location;              /* token location, or -1 if unknown */
} ArrayCoerceExpr;

/* ----------------
* ConvertRowtypeExpr
*
* ConvertRowtypeExpr represents a type coercion from one composite type
* to another, where the source type is guaranteed to contain all the columns
* needed for the destination type plus possibly others; the columns need not
* be in the same positions, but are matched up by name.  This is primarily
* used to convert a whole-row value of an inheritance child table into a
* valid whole-row value of its parent table's rowtype.
* ----------------
*/

typedef struct ConvertRowtypeExpr {
    Expr xpr;
    Expr* arg;      /* input expression */
    Oid resulttype; /* output type (always a composite type) */
    /* Like RowExpr, we deliberately omit a typmod and collation here */
    CoercionForm convertformat; /* how to display this node */
    int location;               /* token location, or -1 if unknown */
} ConvertRowtypeExpr;

/*----------
* CollateExpr - COLLATE
*
* The planner replaces CollateExpr with RelabelType during expression
* preprocessing, so execution never sees a CollateExpr.
*----------
*/
typedef struct CollateExpr {
    Expr xpr;
    Expr* arg;    /* input expression */
    Oid collOid;  /* collation's OID */
    int location; /* token location, or -1 if unknown */
} CollateExpr;

/*----------
* CaseExpr - a CASE expression
*
* We support two distinct forms of CASE expression:
*		CASE WHEN boolexpr THEN expr [ WHEN boolexpr THEN expr ... ]
*		CASE testexpr WHEN compexpr THEN expr [ WHEN compexpr THEN expr ... ]
* These are distinguishable by the "arg" field being NULL in the first case
* and the testexpr in the second case.
*
* In the raw grammar output for the second form, the condition expressions
* of the WHEN clauses are just the comparison values.  Parse analysis
* converts these to valid boolean expressions of the form
*		CaseTestExpr '=' compexpr
* where the CaseTestExpr node is a placeholder that emits the correct
* value at runtime.  This structure is used so that the testexpr need be
* evaluated only once.  Note that after parse analysis, the condition
* expressions always yield boolean.
*
* Note: we can test whether a CaseExpr has been through parse analysis
* yet by checking whether casetype is InvalidOid or not.
*----------
*/
typedef struct CaseExpr {
    Expr xpr;
    Oid casetype;    /* type of expression result */
    Oid casecollid;  /* OID of collation, or InvalidOid if none */
    Expr* arg;       /* implicit equality comparison argument */
    List* args;      /* the arguments (list of WHEN clauses) */
    Expr* defresult; /* the default result (ELSE clause) */
    int location;    /* token location, or -1 if unknown */
} CaseExpr;

/*
* CaseWhen - one arm of a CASE expression
*/
typedef struct CaseWhen {
    Expr xpr;
    Expr* expr;   /* condition expression */
    Expr* result; /* substitution result */
    int location; /* token location, or -1 if unknown */
} CaseWhen;

/*
* Placeholder node for the test value to be processed by a CASE expression.
* This is effectively like a Param, but can be implemented more simply
* since we need only one replacement value at a time.
*
* We also use this in nested UPDATE expressions.
* See transformAssignmentIndirection().
*/
typedef struct CaseTestExpr {
    Expr xpr;
    Oid typeId;      /* type for substituted value */
    int32_t typeMod; /* typemod for substituted value */
    Oid collation;   /* collation for the substituted value */
} CaseTestExpr;

/*
* ArrayExpr - an ARRAY[] expression
*
* Note: if multidims is false, the constituent expressions all yield the
* scalar type identified by element_typeid.  If multidims is true, the
* constituent expressions all yield arrays of element_typeid (ie, the same
* type as array_typeid); at runtime we must check for compatible subscripts.
*/
typedef struct ArrayExpr {
    Expr xpr;
    Oid array_typeid;   /* type of expression result */
    Oid array_collid;   /* OID of collation, or InvalidOid if none */
    Oid element_typeid; /* common type of array elements */
    List* elements;     /* the array elements or sub-arrays */
    bool multidims;     /* true if elements are sub-arrays */
    int location;       /* token location, or -1 if unknown */
} ArrayExpr;

/*
* RowExpr - a ROW() expression
*
* Note: the list of fields must have a one-for-one correspondence with
* physical fields of the associated rowtype, although it is okay for it
* to be shorter than the rowtype.  That is, the N'th list element must
* match up with the N'th physical field.  When the N'th physical field
* is a dropped column (attisdropped) then the N'th list element can just
* be a NULL constant.  (This case can only occur for named composite types,
* not RECORD types, since those are built from the RowExpr itself rather
* than vice versa.)  It is important not to assume that length(args) is
* the same as the number of columns logically present in the rowtype.
*
* colnames provides field names in cases where the names can't easily be
* obtained otherwise.  Names *must* be provided if row_typeid is RECORDOID.
* If row_typeid identifies a known composite type, colnames can be NIL to
* indicate the type's cataloged field names apply.  Note that colnames can
* be non-NIL even for a composite type, and typically is when the RowExpr
* was created by expanding a whole-row Var.  This is so that we can retain
* the column alias names of the RTE that the Var referenced (which would
* otherwise be very difficult to extract from the parsetree).  Like the
* args list, colnames is one-for-one with physical fields of the rowtype.
*/
typedef struct RowExpr {
    Expr xpr;
    List* args;     /* the fields */
    Oid row_typeid; /* RECORDOID or a composite type's ID */

    /*
    * Note: we deliberately do NOT store a typmod.  Although a typmod will be
    * associated with specific RECORD types at runtime, it will differ for
    * different backends, and so cannot safely be stored in stored
    * parsetrees.  We must assume typmod -1 for a RowExpr node.
    *
    * We don't need to store a collation either.  The result type is
    * necessarily composite, and composite types never have a collation.
    */
    CoercionForm row_format; /* how to display this node */
    List* colnames;          /* list of String, or NIL */
    int location;            /* token location, or -1 if unknown */
} RowExpr;

/*
* RowCompareExpr - row-wise comparison, such as (a, b) <= (1, 2)
*
* We support row comparison for any operator that can be determined to
* act like =, <>, <, <=, >, or >= (we determine this by looking for the
* operator in btree opfamilies).  Note that the same operator name might
* map to a different operator for each pair of row elements, since the
* element datatypes can vary.
*
* A RowCompareExpr node is only generated for the < <= > >= cases;
* the = and <> cases are translated to simple AND or OR combinations
* of the pairwise comparisons.  However, we include = and <> in the
* RowCompareType enum for the convenience of parser logic.
*/
typedef enum RowCompareType
{
    /* Values of this enum are chosen to match btree strategy numbers */
    ROWCOMPARE_LT = 1, /* BTLessStrategyNumber */
    ROWCOMPARE_LE = 2, /* BTLessEqualStrategyNumber */
    ROWCOMPARE_EQ = 3, /* BTEqualStrategyNumber */
    ROWCOMPARE_GE = 4, /* BTGreaterEqualStrategyNumber */
    ROWCOMPARE_GT = 5, /* BTGreaterStrategyNumber */
    ROWCOMPARE_NE = 6  /* no such btree strategy */
} RowCompareType;

typedef struct RowCompareExpr {
    Expr xpr;
    RowCompareType rctype; /* LT LE GE or GT, never EQ or NE */
    List* opnos;           /* OID list of pairwise comparison ops */
    List* opfamilies;      /* OID list of containing operator families */
    List* inputcollids;    /* OID list of collations for comparisons */
    List* largs;           /* the left-hand input arguments */
    List* rargs;           /* the right-hand input arguments */
} RowCompareExpr;

/*
* CoalesceExpr - a COALESCE expression
*/
typedef struct CoalesceExpr {
    Expr xpr;
    Oid coalescetype;   /* type of expression result */
    Oid coalescecollid; /* OID of collation, or InvalidOid if none */
    List* args;         /* the arguments */
    int location;       /* token location, or -1 if unknown */
} CoalesceExpr;

/*
* MinMaxExpr - a GREATEST or LEAST function
*/
typedef enum MinMaxOp
{
    IS_GREATEST,
    IS_LEAST
} MinMaxOp;

typedef struct MinMaxExpr {
    Expr xpr;
    Oid minmaxtype;   /* common type of arguments and result */
    Oid minmaxcollid; /* OID of collation of result */
    Oid inputcollid;  /* OID of collation that function should use */
    MinMaxOp op;      /* function to execute */
    List* args;       /* the arguments */
    int location;     /* token location, or -1 if unknown */
} MinMaxExpr;

/*
* XmlExpr - various SQL/XML functions requiring special grammar productions
*
* 'name' carries the "NAME foo" argument (already XML-escaped).
* 'named_args' and 'arg_names' represent an xml_attribute list.
* 'args' carries all other arguments.
*
* Note: result type/typmod/collation are not stored, but can be deduced
* from the XmlExprOp.  The type/typmod fields are just used for display
* purposes, and are NOT necessarily the true result type of the node.
* (We also use type == InvalidOid to mark a not-yet-parse-analyzed XmlExpr.)
*/
typedef enum XmlExprOp
{
    IS_XMLCONCAT,    /* XMLCONCAT(args) */
    IS_XMLELEMENT,   /* XMLELEMENT(name, xml_attributes, args) */
    IS_XMLFOREST,    /* XMLFOREST(xml_attributes) */
    IS_XMLPARSE,     /* XMLPARSE(text, is_doc, preserve_ws) */
    IS_XMLPI,        /* XMLPI(name [, args]) */
    IS_XMLROOT,      /* XMLROOT(xml, version, standalone) */
    IS_XMLSERIALIZE, /* XMLSERIALIZE(is_document, xmlval) */
    IS_DOCUMENT      /* xmlval IS DOCUMENT */
} XmlExprOp;

typedef enum
{
    XMLOPTION_DOCUMENT,
    XMLOPTION_CONTENT
} XmlOptionType;

typedef struct XmlExpr {
    Expr xpr;
    XmlExprOp op;            /* xml function ID */
    char* name;              /* name in xml(NAME foo ...) syntaxes */
    List* named_args;        /* non-XML expressions for xml_attributes */
    List* arg_names;         /* parallel list of Value strings */
    List* args;              /* list of expressions */
    XmlOptionType xmloption; /* DOCUMENT or CONTENT */
    Oid type;                /* target type/typmod for XMLSERIALIZE */
    int32_t typmod;
    int location; /* token location, or -1 if unknown */
} XmlExpr;

/* ----------------
* NullTest
*
* NullTest represents the operation of testing a value for NULLness.
* The appropriate test is performed and returned as a boolean Datum.
*
* When argisrow is false, this simply represents a test for the null value.
*
* When argisrow is true, the input expression must yield a rowtype, and
* the node implements "row IS [NOT] NULL" per the SQL standard.  This
* includes checking individual fields for NULLness when the row datum
* itself isn't NULL.
*
* NOTE: the combination of a rowtype input and argisrow==false does NOT
* correspond to the SQL notation "row IS [NOT] NULL"; instead, this case
* represents the SQL notation "row IS [NOT] DISTINCT FROM NULL".
* ----------------
*/

typedef enum NullTestType
{
    IS_NULL,
    IS_NOT_NULL
} NullTestType;

typedef struct NullTest {
    Expr xpr;
    Expr* arg;                 /* input expression */
    NullTestType nulltesttype; /* IS NULL, IS NOT NULL */
    bool argisrow;             /* T to perform field-by-field null checks */
} NullTest;

/*
* BooleanTest
*
* BooleanTest represents the operation of determining whether a boolean
* is TRUE, FALSE, or UNKNOWN (ie, NULL).  All six meaningful combinations
* are supported.  Note that a NULL input does *not* cause a NULL result.
* The appropriate test is performed and returned as a boolean Datum.
*/

typedef enum BoolTestType
{
    IS_TRUE,
    IS_NOT_TRUE,
    IS_FALSE,
    IS_NOT_FALSE,
    IS_UNKNOWN,
    IS_NOT_UNKNOWN
} BoolTestType;

typedef struct BooleanTest {
    Expr xpr;
    Expr* arg;                 /* input expression */
    BoolTestType booltesttype; /* test type */
} BooleanTest;

/*
* CoerceToDomain
*
* CoerceToDomain represents the operation of coercing a value to a domain
* type.  At runtime (and not before) the precise set of constraints to be
* checked will be determined.  If the value passes, it is returned as the
* result; if not, an error is raised.  Note that this is equivalent to
* RelabelType in the scenario where no constraints are applied.
*/
typedef struct CoerceToDomain {
    Expr xpr;
    Expr* arg;                   /* input expression */
    Oid resulttype;              /* domain type ID (result type) */
    int32_t resulttypmod;        /* output typmod (currently always -1) */
    Oid resultcollid;            /* OID of collation, or InvalidOid if none */
    CoercionForm coercionformat; /* how to display this node */
    int location;                /* token location, or -1 if unknown */
} CoerceToDomain;

/*
* Placeholder node for the value to be processed by a domain's check
* constraint.  This is effectively like a Param, but can be implemented more
* simply since we need only one replacement value at a time.
*
* Note: the typeId/typeMod/collation will be set from the domain's base type,
* not the domain itself.  This is because we shouldn't consider the value
* to be a member of the domain if we haven't yet checked its constraints.
*/
typedef struct CoerceToDomainValue {
    Expr xpr;
    Oid typeId;      /* type for substituted value */
    int32_t typeMod; /* typemod for substituted value */
    Oid collation;   /* collation for the substituted value */
    int location;    /* token location, or -1 if unknown */
} CoerceToDomainValue;

/*
* Placeholder node for a DEFAULT marker in an INSERT or UPDATE command.
*
* This is not an executable expression: it must be replaced by the actual
* column default expression during rewriting.  But it is convenient to
* treat it as an expression node during parsing and rewriting.
*/
typedef struct SetToDefault {
    Expr xpr;
    Oid typeId;      /* type for substituted value */
    int32_t typeMod; /* typemod for substituted value */
    Oid collation;   /* collation for the substituted value */
    int location;    /* token location, or -1 if unknown */
} SetToDefault;

/*
* Node representing [WHERE] CURRENT OF cursor_name
*
* CURRENT OF is a bit like a Var, in that it carries the rangetable index
* of the target relation being constrained; this aids placing the expression
* correctly during planning.  We can assume however that its "levelsup" is
* always zero, due to the syntactic constraints on where it can appear.
*
* CURRENT OF is a bit like a stable function, in that it must be evaluated
* once during constant folding to give the QEs a consistent view of the query.
* To accomplish this, during constant folding, we evaluate the CURRENT OF
* expression into constant values of gp_segment_id, ctid, and tableoid; then,
* we bind these constant values into the CurrentOfExpr here for consumption
* by the QEs.
*/
typedef struct CurrentOfExpr {
    Expr xpr;
    char* cursor_name; /* name of referenced cursor, or NULL */
    int cursor_param;  /* refcursor parameter number, or 0 */
    /* for planning */
    Index cvarno; /* RT index of target relation */
    /* for validation */
    Oid target_relid; /* OID of original target relation,
                                * before any inheritance expansion */
} CurrentOfExpr;

/*--------------------
* TargetEntry -
*	   a target entry (used in query target lists)
*
* Strictly speaking, a TargetEntry isn't an expression node (since it can't
* be evaluated by ExecEvalExpr).  But we treat it as one anyway, since in
* very many places it's convenient to process a whole query targetlist as a
* single expression tree.
*
* In a SELECT's targetlist, resno should always be equal to the item's
* ordinal position (counting from 1).  However, in an INSERT or UPDATE
* targetlist, resno represents the attribute number of the destination
* column for the item; so there may be missing or out-of-order resnos.
* It is even legal to have duplicated resnos; consider
*		UPDATE table SET arraycol[1] = ..., arraycol[2] = ..., ...
* The two meanings come together in the executor, because the planner
* transforms INSERT/UPDATE tlists into a normalized form with exactly
* one entry for each column of the destination table.  Before that's
* happened, however, it is risky to assume that resno == position.
* Generally get_tle_by_resno() should be used rather than list_nth()
* to fetch tlist entries by resno, and only in SELECT should you assume
* that resno is a unique identifier.
*
* resname is required to represent the correct column name in non-resjunk
* entries of top-level SELECT targetlists, since it will be used as the
* column title sent to the frontend.  In most other contexts it is only
* a debugging aid, and may be wrong or even NULL.  (In particular, it may
* be wrong in a tlist from a stored rule, if the referenced column has been
* renamed by ALTER TABLE since the rule was made.  Also, the planner tends
* to store NULL rather than look up a valid name for tlist entries in
* non-toplevel plan nodes.)  In resjunk entries, resname should be either
* a specific system-generated name (such as "ctid") or NULL; anything else
* risks confusing ExecGetJunkAttribute!
*
* ressortgroupref is used in the representation of ORDER BY, GROUP BY, and
* DISTINCT items.  Targetlist entries with ressortgroupref=0 are not
* sort/group items.  If ressortgroupref>0, then this item is an ORDER BY,
* GROUP BY, and/or DISTINCT target value.  No two entries in a targetlist
* may have the same nonzero ressortgroupref --- but there is no particular
* meaning to the nonzero values, except as tags.  (For example, one must
* not assume that lower ressortgroupref means a more significant sort key.)
* The order of the associated SortGroupClause lists determine the semantics.
*
* resorigtbl/resorigcol identify the source of the column, if it is a
* simple reference to a column of a base table (or view).  If it is not
* a simple reference, these fields are zeroes.
*
* If resjunk is true then the column is a working column (such as a sort key)
* that should be removed from the final output of the query.  Resjunk columns
* must have resnos that cannot duplicate any regular column's resno.  Also
* note that there are places that assume resjunk columns come after non-junk
* columns.
*--------------------
*/
typedef struct TargetEntry {
    Expr xpr;
    Expr* expr;            /* expression to evaluate */
    AttrNumber resno;      /* attribute number (see notes above) */
    char* resname;         /* name of the column (could be NULL) */
    Index ressortgroupref; /* nonzero if referenced by a sort/group
                                * clause */
    Oid resorigtbl;        /* OID of column's source table */
    AttrNumber resorigcol; /* column's number in source table */
    bool resjunk;          /* set to true to eliminate the attribute from
                                * final target list */
} TargetEntry;

/* ----------------------------------------------------------------
*					node types for join trees
*
* The leaves of a join tree structure are RangeTblRef nodes.  Above
* these, JoinExpr nodes can appear to denote a specific kind of join
* or qualified join.  Also, FromExpr nodes can appear to denote an
* ordinary cross-product join ("FROM foo, bar, baz WHERE ...").
* FromExpr is like a JoinExpr of jointype JOIN_INNER, except that it
* may have any number of child nodes, not just two.
*
* NOTE: the top level of a Query's jointree is always a FromExpr.
* Even if the jointree contains no rels, there will be a FromExpr.
*
* NOTE: the qualification expressions present in JoinExpr nodes are
* *in addition to* the query's main WHERE clause, which appears as the
* qual of the top-level FromExpr.  The reason for associating quals with
* specific nodes in the jointree is that the position of a qual is critical
* when outer joins are present.  (If we enforce a qual too soon or too late,
* that may cause the outer join to produce the wrong set of NULL-extended
* rows.)  If all joins are inner joins then all the qual positions are
* semantically interchangeable.
*
* NOTE: in the raw output of gram.y, a join tree contains RangeVar,
* RangeSubselect, and RangeFunction nodes, which are all replaced by
* RangeTblRef nodes during the parse analysis phase.  Also, the top-level
* FromExpr is added during parse analysis; the grammar regards FROM and
* WHERE as separate.
* ----------------------------------------------------------------
*/

/*
* RangeTblRef - reference to an entry in the query's rangetable
*
* We could use direct pointers to the RT entries and skip having these
* nodes, but multiple pointers to the same node in a querytree cause
* lots of headaches, so it seems better to store an index into the RT.
*/
typedef struct RangeTblRef {
    NodeTag type;
    int rtindex;
} RangeTblRef;

/*----------
* JoinExpr - for SQL JOIN expressions
*
* isNatural, usingClause, and quals are interdependent.  The user can write
* only one of NATURAL, USING(), or ON() (this is enforced by the grammar).
* If he writes NATURAL then parse analysis generates the equivalent USING()
* list, and from that fills in "quals" with the right equality comparisons.
* If he writes USING() then "quals" is filled with equality comparisons.
* If he writes ON() then only "quals" is set.  Note that NATURAL/USING
* are not equivalent to ON() since they also affect the output column list.
*
* alias is an Alias node representing the AS alias-clause attached to the
* join expression, or NULL if no clause.  NB: presence or absence of the
* alias has a critical impact on semantics, because a join with an alias
* restricts visibility of the tables/columns inside it.
*
* During parse analysis, an RTE is created for the Join, and its index
* is filled into rtindex.  This RTE is present mainly so that Vars can
* be created that refer to the outputs of the join.  The planner sometimes
* generates JoinExprs internally; these can have rtindex = 0 if there are
* no join alias variables referencing such joins.
*----------
*/
typedef struct JoinExpr {
    NodeTag type;
    JoinType jointype; /* type of join */
    bool isNatural;    /* Natural join? Will need to shape table */
    Node* larg;        /* left subtree */
    Node* rarg;        /* right subtree */
    List* usingClause; /* USING clause, if any (list of String) */
    Node* quals;       /* qualifiers on join, if any */
    Alias* alias;      /* user-written alias clause, if any */
    int rtindex;       /* RT index assigned for join, or 0 */
} JoinExpr;

/*----------
* FromExpr - represents a FROM ... WHERE ... construct
*
* This is both more flexible than a JoinExpr (it can have any number of
* children, including zero) and less so --- we don't need to deal with
* aliases and so on.  The output column set is implicitly just the union
* of the outputs of the children.
*----------
*/
typedef struct FromExpr {
    NodeTag type;
    List* fromlist; /* List of join subtrees */
    Node* quals;    /* qualifiers on join, if any */
} FromExpr;

typedef enum Movement
{
    MOVEMENT_NONE,         /* No motion required. */
    MOVEMENT_FOCUS,        /* Fixed motion to a single segment. */
    MOVEMENT_BROADCAST,    /* Broadcast motion. */
    MOVEMENT_REPARTITION,  /* Hash motion */
    MOVEMENT_LIM_RESTRUCT, /* Restructure a Limit node into three stages */
    MOVEMENT_EXPLICIT      /* Move tuples to the segments specified in the segid column */
} Movement;

// mdxn: commented
/*----------
* Flow - describes a tuple flow in a parallelized plan
*
* This node type is a MPP extension.
*
* Plan nodes contain a reference to a Flow that characterizes the output
* tuple flow of the node.  In addition, the node contains fields used for
* parallelizing specification.
*----------
*/
//typedef struct Flow
//{
//   NodeTag		type;			/* T_Flow */
//   FlowType	flotype;		/* Type of flow produced by the plan. */
//
//   /* What motion (including none) should be applied to this Plan's output. */
//   Movement	req_move;
//
//   /* Locus type (optimizer flow characterization).
//    */
//   CdbLocusType	locustype;
//
//   /* If flotype is FLOW_SINGLETON, then this is the segment (-1 for entry)
//    * on which tuples occur.  If req_move is MOVEMENT_FOCUS, then this is
//    * the desired segment for the resulting singleton flow.
//    */
//   int			segindex;		/* Segment index of singleton flow. */
//   int         numsegments;
//
//   /* If req_move is MOVEMENT_REPARTITION, these express the desired
//    * partitioning for a hash motion.  Else if flotype is FLOW_PARTITIONED,
//    * this is the partitioning key.  Otherwise NIL.
//    * otherwise, they are NIL. */
//   List       *hashExprs;			/* list of hash expressions */
//   List	   *hashOpfamilies;
//
//   /* If req_move is MOVEMENT_EXPLICIT, this contains the index of the segid column
//    * to use in the motion	 */
//   AttrNumber segidColIdx;
//
//   /* The original Flow ptr is saved here upon setting req_move. */
//   struct Flow    *flow_before_req_move;
//
//} Flow;

typedef enum GroupingType
{
    GROUPINGTYPE_ROLLUP,       /* ROLLUP grouping extension */
    GROUPINGTYPE_CUBE,         /* CUBE grouping extension */
    GROUPINGTYPE_GROUPING_SETS /* GROUPING SETS grouping extension */
} GroupingType;

/*
* DMLActionExpr
*
* Represents the expression which introduces the action in a SplitUpdate statement
*/
typedef struct DMLActionExpr {
    Expr xpr;
} DMLActionExpr;

/*
* PartSelectedExpr
*
* Returns true, if the PartitionSelector has selected this partition.
*/
typedef struct PartSelectedExpr {
    Expr xpr;
    int dynamicScanId;
    Oid partOid;
} PartSelectedExpr;

/*
* PartDefaultExpr
* Represents the expression which determines whether this a part is a default part
*/
typedef struct PartDefaultExpr {
    Expr xpr;
    int level; /* partitioning level */
} PartDefaultExpr;

/*
* PartBoundExpr
* Represents the expression which holds a part boundary in a PartitionSelector operator
*/
typedef struct PartBoundExpr {
    Expr xpr;
    int level;         /* partitioning level */
    Oid boundType;     /* the return type of this boundary - same as part key */
    bool isLowerBound; /* lower (min) or upper (max) bound */
} PartBoundExpr;

/*
* PartBoundInclusionExpr
* Represents the expression which determines whether a part boundary is inclusive or not
* in a PartitionSelector operator
*/
typedef struct PartBoundInclusionExpr {
    Expr xpr;
    int level;         /* partitioning level */
    bool isLowerBound; /* lower (min) or upper (max) bound */
} PartBoundInclusionExpr;

/*
* PartBoundOpenExpr
* Represents the expression which determines whether a part boundary is open (unbounded) or not
* in a PartitionSelector operator
*/
typedef struct PartBoundOpenExpr {
    Expr xpr;
    int level;         /* partitioning level */
    bool isLowerBound; /* lower (min) or upper (max) bound */
} PartBoundOpenExpr;

/*
* PartListRuleExpr
* Represents the expression that converts the current rule for
* level "level" to a list of constants. It only appears in
* levelExpressions (which is non-equality predicates) for
* the partition selector.
*/
typedef struct PartListRuleExpr {
    Expr xpr;
    int level;       /* partitioning level */
    Oid resulttype;  /* the result type of expr - array type of part key */
    Oid elementtype; /* the element type of partition list values */
} PartListRuleExpr;

/*
* PartListNullTestExpr
*
* Represents whether the list values of a partition for the specified level
* contains NULL or not, if nulltesttype is IS_NULL.
*
* NOTE: This expr only works for list partition.
*/
typedef struct PartListNullTestExpr {
    Expr xpr;
    int level;                 /* partitioning level */
    NullTestType nulltesttype; /* IS NULL, IS NOT NULL */
} PartListNullTestExpr;

#endif /* PRIMNODES_H */