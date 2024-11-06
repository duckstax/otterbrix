#pragma once

#include "sql_new/parser/nodes/nodes.h"
#include "sql_new/transformer/expressions/expression_enums.hpp"
#include <string>

namespace components::sql_new::transform {
    template<class T>
    static T& pg_cast(Node& node) {
        return reinterpret_cast<T&>(node);
    }

    template<class T>
    static T* pg_ptr_cast(void* ptr) {
        return reinterpret_cast<T*>(ptr);
    }

    bool string_to_double(const char* buf, size_t len, double& result /*, char decimal_separator*/);

    std::string node_tag_to_string(NodeTag type);

    namespace expressions {
        ExpressionType negate_comparison_type(ExpressionType type);
    }
} // namespace components::sql_new