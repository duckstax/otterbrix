#include "create_plan_match.hpp"
#include <services/collection/operators/scan/full_scan.hpp>
#include <iostream> //todo: delete

namespace services::collection::planner::impl {

    operators::operator_ptr create_plan_match(const components::logical_plan::node_ptr& node) {
        std::cout << node->to_string() << std::endl;
        return nullptr;
        //return std::make_unique<operators::full_scan>(context_collection_t* collection, predicates::predicate_ptr predicate, predicates::limit_t limit);
    }

}