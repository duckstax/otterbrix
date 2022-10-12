#include "scan.hpp"
#include "full_scan.hpp"

namespace services::collection::operators {

    operator_ptr create_searcher(context_collection_t* context, components::ql::find_statement &statement, predicates::limit_t limit) {
        //todo: choice searcher
        return std::make_unique<full_scan>(context, predicates::create_predicate(context, statement), limit);
    }

} // namespace services::collection::operators
