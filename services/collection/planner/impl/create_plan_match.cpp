#include "create_plan_match.hpp"
#include <services/collection/operators/scan/full_scan.hpp>
#include <services/collection/operators/scan/transfer_scan.hpp>

namespace services::collection::planner::impl {

    operators::operator_ptr create_plan_match(context_collection_t* context, const components::logical_plan::node_ptr& node) {
        operators::predicates::limit_t limit = operators::predicates::limit_t::unlimit(); //todo: set limit
        if (node->expressions().empty()) {
            return std::make_unique<operators::transfer_scan>(context, limit);
        } else { //todo: other kinds scan
            operators::predicates::predicate_ptr predicate;
            return std::make_unique<operators::full_scan>(context, std::move(predicate), limit);
        }
    }

}