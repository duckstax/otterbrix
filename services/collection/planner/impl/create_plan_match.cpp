#include "create_plan_match.hpp"
#include <components/expressions/compare_expression.hpp>
#include <services/collection/operators/scan/full_scan.hpp>
#include <services/collection/operators/scan/transfer_scan.hpp>

namespace services::collection::planner::impl {

    operators::operator_ptr create_plan_match(
            context_collection_t* context,
            const components::logical_plan::node_ptr& node,
            operators::predicates::limit_t limit) {
        if (node->expressions().empty()) {
            return std::make_unique<operators::transfer_scan>(context, std::move(limit));
        } else { //todo: other kinds scan
            auto expr = reinterpret_cast<const components::expressions::compare_expression_ptr&>(node->expressions().at(0));
            auto predicate = operators::predicates::create_predicate(context, expr);
            return std::make_unique<operators::full_scan>(context, std::move(predicate), std::move(limit));
        }
    }

}
