#pragma once

#include <components/expressions/compare_expression.hpp>
#include <services/collection/operators/operator.hpp>
#include <services/collection/operators/predicates/limit.hpp>

namespace services::collection::operators {

    operator_ptr create_searcher(context_collection_t* context, const components::expressions::compare_expression_ptr& expr, predicates::limit_t limit);

} // namespace services::collection::operators
