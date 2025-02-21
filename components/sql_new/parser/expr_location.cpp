#include "pg_functions.h"

static int leftmostLoc(int loc1, int loc2) {
    if (loc1 < 0)
        return loc2;
    else if (loc2 < 0)
        return loc1;
    else
        return (loc1 < loc2 ? loc1 : loc2);
}

int exprLocation(const Node* expr) {
    int loc;

    if (expr == NULL)
        return -1;
    switch (nodeTag(expr)) {
        case T_RangeVar:
            loc = ((const RangeVar*) expr)->location;
            break;
        case T_Var:
            loc = ((const Var*) expr)->location;
            break;
        case T_Const:
            loc = ((const Const*) expr)->location;
            break;
        case T_Param:
            loc = ((const Param*) expr)->location;
            break;
        case T_Aggref:
            /* function name should always be the first thing */
            loc = ((const Aggref*) expr)->location;
            break;
        case T_WindowFunc:
            /* function name should always be the first thing */
            loc = ((const WindowFunc*) expr)->location;
            break;
        case T_ArrayRef:
            /* just use array argument's location */
            loc = exprLocation((Node*) ((const ArrayRef*) expr)->refexpr);
            break;
        case T_FuncExpr: {
            const FuncExpr* fexpr = (const FuncExpr*) expr;

            /* consider both function name and leftmost arg */
            loc = leftmostLoc(fexpr->location, exprLocation((Node*) fexpr->args));
        } break;
        case T_NamedArgExpr: {
            const NamedArgExpr* na = (const NamedArgExpr*) expr;

            /* consider both argument name and value */
            loc = leftmostLoc(na->location, exprLocation((Node*) na->arg));
        } break;
        case T_OpExpr:
        case T_DistinctExpr: /* struct-equivalent to OpExpr */
        case T_NullIfExpr:   /* struct-equivalent to OpExpr */
        {
            const OpExpr* opexpr = (const OpExpr*) expr;

            /* consider both operator name and leftmost arg */
            loc = leftmostLoc(opexpr->location, exprLocation((Node*) opexpr->args));
        } break;
        case T_ScalarArrayOpExpr: {
            const ScalarArrayOpExpr* saopexpr = (const ScalarArrayOpExpr*) expr;

            /* consider both operator name and leftmost arg */
            loc = leftmostLoc(saopexpr->location, exprLocation((Node*) saopexpr->args));
        } break;
        case T_BoolExpr: {
            const BoolExpr* bexpr = (const BoolExpr*) expr;

            /*
				 * Same as above, to handle either NOT or AND/OR.  We can't
				 * special-case NOT because of the way that it's used for
				 * things like IS NOT BETWEEN.
				 */
            loc = leftmostLoc(bexpr->location, exprLocation((Node*) bexpr->args));
        } break;
        case T_SubLink: {
            const SubLink* sublink = (const SubLink*) expr;

            /* check the testexpr, if any, and the operator/keyword */
            loc = leftmostLoc(exprLocation(sublink->testexpr), sublink->location);
        } break;
        case T_FieldSelect:
            /* just use argument's location */
            loc = exprLocation((Node*) ((const FieldSelect*) expr)->arg);
            break;
        case T_FieldStore:
            /* just use argument's location */
            loc = exprLocation((Node*) ((const FieldStore*) expr)->arg);
            break;
        case T_RelabelType: {
            const RelabelType* rexpr = (const RelabelType*) expr;

            /* Much as above */
            loc = leftmostLoc(rexpr->location, exprLocation((Node*) rexpr->arg));
        } break;
        case T_CoerceViaIO: {
            const CoerceViaIO* cexpr = (const CoerceViaIO*) expr;

            /* Much as above */
            loc = leftmostLoc(cexpr->location, exprLocation((Node*) cexpr->arg));
        } break;
        case T_ArrayCoerceExpr: {
            const ArrayCoerceExpr* cexpr = (const ArrayCoerceExpr*) expr;

            /* Much as above */
            loc = leftmostLoc(cexpr->location, exprLocation((Node*) cexpr->arg));
        } break;
        case T_ConvertRowtypeExpr: {
            const ConvertRowtypeExpr* cexpr = (const ConvertRowtypeExpr*) expr;

            /* Much as above */
            loc = leftmostLoc(cexpr->location, exprLocation((Node*) cexpr->arg));
        } break;
        case T_CollateExpr:
            /* just use argument's location */
            loc = exprLocation((Node*) ((const CollateExpr*) expr)->arg);
            break;
        case T_CaseExpr:
            /* CASE keyword should always be the first thing */
            loc = ((const CaseExpr*) expr)->location;
            break;
        case T_CaseWhen:
            /* WHEN keyword should always be the first thing */
            loc = ((const CaseWhen*) expr)->location;
            break;
        case T_ArrayExpr:
            /* the location points at ARRAY or [, which must be leftmost */
            loc = ((const ArrayExpr*) expr)->location;
            break;
        case T_RowExpr:
            /* the location points at ROW or (, which must be leftmost */
            loc = ((const RowExpr*) expr)->location;
            break;
        case T_TableValueExpr:
            /* the location points at TABLE, which must be leftmost */
            loc = ((TableValueExpr*) expr)->location;
            break;
        case T_RowCompareExpr:
            /* just use leftmost argument's location */
            loc = exprLocation((Node*) ((const RowCompareExpr*) expr)->largs);
            break;
        case T_CoalesceExpr:
            /* COALESCE keyword should always be the first thing */
            loc = ((const CoalesceExpr*) expr)->location;
            break;
        case T_MinMaxExpr:
            /* GREATEST/LEAST keyword should always be the first thing */
            loc = ((const MinMaxExpr*) expr)->location;
            break;
        case T_XmlExpr: {
            const XmlExpr* xexpr = (const XmlExpr*) expr;

            /* consider both function name and leftmost arg */
            loc = leftmostLoc(xexpr->location, exprLocation((Node*) xexpr->args));
        } break;
        case T_NullTest:
            /* just use argument's location */
            loc = exprLocation((Node*) ((const NullTest*) expr)->arg);
            break;
        case T_BooleanTest:
            /* just use argument's location */
            loc = exprLocation((Node*) ((const BooleanTest*) expr)->arg);
            break;
        case T_CoerceToDomain: {
            const CoerceToDomain* cexpr = (const CoerceToDomain*) expr;

            /* Much as above */
            loc = leftmostLoc(cexpr->location, exprLocation((Node*) cexpr->arg));
        } break;
        case T_CoerceToDomainValue:
            loc = ((const CoerceToDomainValue*) expr)->location;
            break;
        case T_SetToDefault:
            loc = ((const SetToDefault*) expr)->location;
            break;
        case T_TargetEntry:
            /* just use argument's location */
            loc = exprLocation((Node*) ((const TargetEntry*) expr)->expr);
            break;
        case T_IntoClause:
            /* use the contained RangeVar's location --- close enough */
            loc = exprLocation((Node*) ((const IntoClause*) expr)->rel);
            break;
        case T_List: {
            /* report location of first list member that has a location */
            ListCell* lc;

            loc = -1; /* just to suppress compiler warning */
                      //            foreach (lc, (const List*) expr) {
                      //                loc = exprLocation((Node*) lfirst(lc));
                      //                if (loc >= 0)
                      //                    break;
                      //            }
            for (auto i : ((const List*) expr)->lst) {
                loc = exprLocation((Node*) i.data);
                if (loc >= 0)
                    break;
            }
        } break;
        case T_A_Expr: {
            const A_Expr* aexpr = (const A_Expr*) expr;

            /* use leftmost of operator or left operand (if any) */
            /* we assume right operand can't be to left of operator */
            loc = leftmostLoc(aexpr->location, exprLocation(aexpr->lexpr));
        } break;
        case T_ColumnRef:
            loc = ((const ColumnRef*) expr)->location;
            break;
        case T_ParamRef:
            loc = ((const ParamRef*) expr)->location;
            break;
        case T_A_Const:
            loc = ((const A_Const*) expr)->location;
            break;
        case T_FuncCall: {
            const FuncCall* fc = (const FuncCall*) expr;

            /* consider both function name and leftmost arg */
            /* (we assume any ORDER BY nodes must be to right of name) */
            loc = leftmostLoc(fc->location, exprLocation((Node*) fc->args));
        } break;
        case T_A_ArrayExpr:
            /* the location points at ARRAY or [, which must be leftmost */
            loc = ((const A_ArrayExpr*) expr)->location;
            break;
        case T_ResTarget:
            /* we need not examine the contained expression (if any) */
            loc = ((const ResTarget*) expr)->location;
            break;
        case T_TypeCast: {
            const TypeCast* tc = (const TypeCast*) expr;

            /*
				 * This could represent CAST(), ::, or TypeName 'literal', so
				 * any of the components might be leftmost.
				 */
            loc = exprLocation(tc->arg);
            loc = leftmostLoc(loc, tc->typeName->location);
            loc = leftmostLoc(loc, tc->location);
        } break;
        case T_CollateClause:
            /* just use argument's location */
            loc = exprLocation(((const CollateClause*) expr)->arg);
            break;
        case T_SortBy:
            /* just use argument's location (ignore operator, if any) */
            loc = exprLocation(((const SortBy*) expr)->node);
            break;
        case T_WindowDef:
            loc = ((const WindowDef*) expr)->location;
            break;
        case T_TypeName:
            loc = ((const TypeName*) expr)->location;
            break;
        case T_ColumnDef:
            loc = ((const ColumnDef*) expr)->location;
            break;
        case T_Constraint:
            loc = ((const Constraint*) expr)->location;
            break;
        case T_FunctionParameter:
            /* just use typename's location */
            loc = exprLocation((Node*) ((const FunctionParameter*) expr)->argType);
            break;
        case T_XmlSerialize:
            /* XMLSERIALIZE keyword should always be the first thing */
            loc = ((const XmlSerialize*) expr)->location;
            break;
        case T_WithClause:
            loc = ((const WithClause*) expr)->location;
            break;
        case T_CommonTableExpr:
            loc = ((const CommonTableExpr*) expr)->location;
            break;
            //        case T_PlaceHolderVar:
            /* just use argument's location */ //mdxn: unused?
            //            loc = exprLocation((Node*) ((const PlaceHolderVar*) expr)->phexpr);
            //            break;
        default:
            /* for any other node type it's just unknown... */
            loc = -1;
            break;
    }
    return loc;
}
