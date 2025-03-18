#include "makefuncs.h"
#include <components/sql/parser/pg_functions.h>

/*
 * makeA_Expr -
 *		makes an A_Expr node
 */
A_Expr* makeA_Expr(A_Expr_Kind kind, List* name, Node* lexpr, Node* rexpr, int location) {
    A_Expr* a = makeNode(A_Expr);

    a->kind = kind;
    a->name = name;
    a->lexpr = lexpr;
    a->rexpr = rexpr;
    a->location = location;
    return a;
}

/*
 * makeSimpleA_Expr -
 *		As above, given a simple (unqualified) operator name
 */
A_Expr* makeSimpleA_Expr(A_Expr_Kind kind, char* name, Node* lexpr, Node* rexpr, int location) {
    A_Expr* a = makeNode(A_Expr);

    a->kind = kind;
    a->name = list_make1(makeString((char*) name));
    a->lexpr = lexpr;
    a->rexpr = rexpr;
    a->location = location;
    return a;
}

/*
 * makeRangeVar -
 *	  creates a RangeVar node (rather oversimplified case)
 */
RangeVar* makeRangeVar(char* schemaname, char* relname, int location) {
    RangeVar* r = makeNode(RangeVar);

    r->catalogname = NULL;
    r->schemaname = schemaname;
    r->relname = relname;
    r->inhOpt = INH_DEFAULT;
    r->relpersistence = RELPERSISTENCE_PERMANENT;
    r->alias = NULL;
    r->location = location;

    return r;
}

/*
 * makeTypeName -
 *	build a TypeName node for an unqualified name.
 *
 * typmod is defaulted, but can be changed later by caller.
 */
TypeName* makeTypeName(char* typnam) { return makeTypeNameFromNameList(list_make1(makeString(typnam))); }

/*
 * makeTypeNameFromNameList -
 *	build a TypeName node for a String list representing a qualified name.
 *
 * typmod is defaulted, but can be changed later by caller.
 */
TypeName* makeTypeNameFromNameList(List* names) {
    TypeName* n = makeNode(TypeName);

    n->names = names;
    n->typmods = NIL;
    n->typemod = -1;
    n->location = -1;
    return n;
}

/*
 * makeDefElem -
 *	build a DefElem node
 *
 * This is sufficient for the "typical" case with an unqualified option name
 * and no special action.
 */
DefElem* makeDefElem(char* name, Node* arg) {
    DefElem* res = makeNode(DefElem);

    res->defnamespace = NULL;
    res->defname = name;
    res->arg = arg;
    res->defaction = DEFELEM_UNSPEC;

    return res;
}

/*
 * makeDefElemExtended -
 *	build a DefElem node with all fields available to be specified
 */
DefElem* makeDefElemExtended(char* nameSpace, char* name, Node* arg, DefElemAction defaction) {
    DefElem* res = makeNode(DefElem);

    res->defnamespace = nameSpace;
    res->defname = name;
    res->arg = arg;
    res->defaction = defaction;

    return res;
}

/*
 * makeFuncCall -
 *
 * Initialize a FuncCall struct with the information every caller must
 * supply.  Any non-default parameters have to be inserted by the caller.
 */
FuncCall* makeFuncCall(List* name, List* args, int location) {
    FuncCall* n = makeNode(FuncCall);

    n->funcname = name;
    n->args = args;
    n->agg_order = NIL;
    n->agg_filter = NULL;
    n->agg_within_group = false;
    n->agg_star = false;
    n->agg_distinct = false;
    n->func_variadic = false;
    n->over = NULL;
    n->location = location;
    return n;
}