#pragma once

#include <services/collection/operators/operator.hpp>
#include <services/collection/operators/predicates/limit.hpp>

namespace services::collection::operators {

    operator_ptr create_searcher(context_collection_t* context, components::ql::find_statement &statement, predicates::limit_t limit);

} // namespace services::collection::operators
