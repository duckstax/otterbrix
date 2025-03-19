#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/expressions/forward.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/sql/parser/nodes/parsenodes.h>
#include <string>

namespace components::sql::transform {
    template<class T>
    static T& pg_cast(Node& node) {
        return reinterpret_cast<T&>(node);
    }

    template<class T>
    static T* pg_ptr_cast(void* ptr) {
        return reinterpret_cast<T*>(ptr);
    }

    static Node& pg_cell_to_node_cast(void* node) { return pg_cast<Node&>(*reinterpret_cast<Node*>(node)); }

    bool string_to_double(const char* buf, size_t len, double& result /*, char decimal_separator*/);

    static collection_full_name_t rangevar_to_collection(RangeVar* table) {
        return {table->schemaname ? table->schemaname : database_name_t(), table->relname};
    }

    static logical_plan::join_type jointype_to_ql(JoinExpr* join) {
        switch (join->jointype) {
            case JOIN_FULL:
                return logical_plan::join_type::full;
            case JOIN_INNER:
                return join->quals ? logical_plan::join_type::inner : logical_plan::join_type::cross;
            case JOIN_LEFT:
                return logical_plan::join_type::left;
            case JOIN_RIGHT:
                return logical_plan::join_type::right;
            default:
                throw std::runtime_error("unsupported join type");
        }
    }

    static components::expressions::compare_type get_compare_type(std::string_view str) {
        static const std::unordered_map<std::string_view, components::expressions::compare_type> lookup = {
            {"==", components::expressions::compare_type::eq},
            {"=", components::expressions::compare_type::eq},
            {"!=", components::expressions::compare_type::ne},
            {"<>", components::expressions::compare_type::ne},
            {"<", components::expressions::compare_type::lt},
            {"<=", components::expressions::compare_type::lte},
            {">", components::expressions::compare_type::gt},
            {">=", components::expressions::compare_type::gte},
            {"regexp", components::expressions::compare_type::regex},
            {"~~", components::expressions::compare_type::regex}};

        if (auto it = lookup.find(str); it != lookup.end()) {
            return it->second;
        }

        throw std::runtime_error("Unknown comparison operator: " + std::string(str));
    }

    static components::expressions::aggregate_type get_aggregate_type(std::string_view str) {
        static const std::unordered_map<std::string_view, components::expressions::aggregate_type> lookup = {
            {"count", components::expressions::aggregate_type::count},
            {"sum", components::expressions::aggregate_type::sum},
            {"min", components::expressions::aggregate_type::min},
            {"max", components::expressions::aggregate_type::max},
            {"avg", components::expressions::aggregate_type::avg},
        };

        if (auto it = lookup.find(str); it != lookup.end()) {
            return it->second;
        }

        return expressions::aggregate_type::invalid;
    }

    std::string node_tag_to_string(NodeTag type);
    std::string expr_kind_to_string(A_Expr_Kind type);
} // namespace components::sql::transform