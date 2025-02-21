#include "sql_new/transformer/utils.hpp"
#include <cstdlib>
#include <stdexcept>

namespace components::sql_new::transform {
    bool string_to_double(const char* buf, size_t len, double& result /*, char decimal_separator = '.'*/) {
        // Skip leading spaces
        while (len > 0 && std::isspace(*buf)) {
            buf++;
            len--;
        }
        if (len == 0) {
            return false;
        }
        if (*buf == '+') {
            buf++;
            len--;
        }

        std::string str(buf, len);
        const char* start = str.c_str();
        char* endptr = nullptr;

        result = std::strtod(start, &endptr);

        if (start == endptr) {
            return false;
        }
        while (*endptr != '\0' && std::isspace(*endptr)) {
            endptr++;
        }

        return *endptr == '\0';
    }

    std::string node_tag_to_string(NodeTag type) {
        switch (type) {
            case T_A_Expr:
                return "T_A_Expr";
            case T_ColumnRef:
                return "T_ColumnRef";
            case T_ParamRef:
                return "T_ParamRef";
            case T_A_Const:
                return "T_A_Const";
            case T_FuncCall:
                return "T_FuncCall";
            case T_A_Star:
                return "T_A_Star";
            case T_A_Indices:
                return "T_A_Indices";
            case T_A_Indirection:
                return "T_A_Indirection";
            case T_A_ArrayExpr:
                return "T_A_ArrayExpr";
            case T_ResTarget:
                return "T_ResTarget";
            case T_TypeCast:
                return "T_TypeCast";
            case T_CollateClause:
                return "T_CollateClause";
            case T_SortBy:
                return "T_SortBy";
            case T_WindowDef:
                return "T_WindowDef";
            case T_RangeSubselect:
                return "T_RangeSubselect";
            case T_RangeFunction:
                return "T_RangeFunction";
            case T_TypeName:
                return "T_TypeName";
            case T_ColumnDef:
                return "T_ColumnDef";
            case T_IndexElem:
                return "T_IndexElem";
            case T_Constraint:
                return "T_Constraint";
            case T_DefElem:
                return "T_DefElem";
            case T_RangeTblEntry:
                return "T_RangeTblEntry";
            case T_RangeTblFunction:
                return "T_RangeTblFunction";
            case T_WithCheckOption:
                return "T_WithCheckOption";
            case T_GroupingClause:
                return "T_GroupingClause";
            case T_GroupingFunc:
                return "T_GroupingFunc";
            case T_SortGroupClause:
                return "T_SortGroupClause";
            case T_WindowClause:
                return "T_WindowClause";
            case T_PrivGrantee:
                return "T_PrivGrantee";
            case T_FuncWithArgs:
                return "T_FuncWithArgs";
            case T_AccessPriv:
                return "T_AccessPriv";
            case T_CreateOpClassItem:
                return "T_CreateOpClassItem";
            case T_TableLikeClause:
                return "T_TableLikeClause";
            case T_FunctionParameter:
                return "T_FunctionParameter";
            case T_LockingClause:
                return "T_LockingClause";
            case T_RowMarkClause:
                return "T_RowMarkClause";
            case T_XmlSerialize:
                return "T_XmlSerialize";
            case T_WithClause:
                return "T_WithClause";
            case T_CommonTableExpr:
                return "T_CommonTableExpr";
            case T_ColumnReferenceStorageDirective:
                return "T_ColumnReferenceStorageDirective";
            default:
                return "unknown";
        }
    }

    namespace expressions {
        ExpressionType negate_comparison_type(ExpressionType type) {
            ExpressionType negated_type = ExpressionType::INVALID;
            switch (type) {
                case ExpressionType::COMPARE_EQUAL:
                    negated_type = ExpressionType::COMPARE_NOTEQUAL;
                    break;
                case ExpressionType::COMPARE_NOTEQUAL:
                    negated_type = ExpressionType::COMPARE_EQUAL;
                    break;
                case ExpressionType::COMPARE_LESSTHAN:
                    negated_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
                    break;
                case ExpressionType::COMPARE_GREATERTHAN:
                    negated_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
                    break;
                case ExpressionType::COMPARE_LESSTHANOREQUALTO:
                    negated_type = ExpressionType::COMPARE_GREATERTHAN;
                    break;
                case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
                    negated_type = ExpressionType::COMPARE_LESSTHAN;
                    break;
                default:
                    throw std::runtime_error("Unsupported comparison type in negation");
            }
            return negated_type;
        }
    } // namespace expressions
} // namespace components::sql_new::transform
