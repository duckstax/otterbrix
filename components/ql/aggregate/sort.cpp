#include "sort.hpp"
#include <components/expressions/sort_expression.hpp>

namespace components::ql::aggregate {

    void append_sort(sort_t& sort, const expressions::key_t& key, expressions::sort_order order) {
        sort.values.push_back(new expressions::sort_expression_t{key, order});
    }

} // namespace components::ql::aggregate
