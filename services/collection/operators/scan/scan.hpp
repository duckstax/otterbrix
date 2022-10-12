#pragma once

#include <services/collection/operators/operator.hpp>
#include <services/collection/operators/predicates/limit.hpp>

namespace services::collection::operators {

    operator_ptr create_searcher(context_collection_t* context, const components::ql::find_statement_ptr& statement, predicates::limit_t limit);

} // namespace services::collection::operators
