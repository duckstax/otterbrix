#include "create_plan_delete.hpp"
#include "create_plan_match.hpp"
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/collection/operators/operator_delete.hpp>
#include <expressions/compare_expression.hpp>
#include <physical_plan/collection/operators/predicates/predicate.hpp>
#include <physical_plan/collection/operators/scan/full_scan.hpp>

#include "create_plan_data.hpp"

namespace services::collection::planner::impl {

    operators::operator_ptr create_plan_delete(const context_storage_t& context,
                                               const components::logical_plan::node_ptr& node) {
        const auto* node_delete = static_cast<const components::logical_plan::node_delete_t*>(node.get());

        components::logical_plan::node_ptr node_match = nullptr;
        components::logical_plan::node_ptr node_limit = nullptr;
        components::logical_plan::node_ptr node_raw_data = nullptr;
        for (auto child : node_delete->children()) {
            if (child->type() == components::logical_plan::node_type::match_t) {
                node_match = child;
            } else if (child->type() == components::logical_plan::node_type::limit_t) {
                node_limit = child;
            } else if (child->type() == components::logical_plan::node_type::data_t) {
                node_raw_data = child;
            }
        }
        auto limit = static_cast<components::logical_plan::node_limit_t*>(node_limit.get())->limit();
        if (node_delete->collection_from().empty()) {
            auto plan = boost::intrusive_ptr(new operators::operator_delete(context.at(node->collection_full_name())));
            plan->set_children(create_plan_match(context, node_match, limit));

            return plan;
        } else {
            auto expr =
                reinterpret_cast<const components::expressions::compare_expression_ptr*>(&node->expressions()[0]);
            auto predicate = operators::predicates::create_predicate(context.at(collection_full_name_t{}), *expr);

            auto plan = boost::intrusive_ptr(
                new operators::operator_delete(context.at(node->collection_full_name()), std::move(predicate)));
            if (node_raw_data) {
                plan->set_children(boost::intrusive_ptr(new operators::full_scan(
                                       context.at(node->collection_full_name()),
                                       operators::predicates::create_all_true_predicate(node->resource()),
                                       limit)),
                                   create_plan_data(node_raw_data));
            } else {
                plan->set_children(boost::intrusive_ptr(new operators::full_scan(
                                       context.at(node->collection_full_name()),
                                       operators::predicates::create_all_true_predicate(node->resource()),
                                       limit)),
                                   boost::intrusive_ptr(new operators::full_scan(
                                       context.at(node->collection_full_name()),
                                       operators::predicates::create_all_true_predicate(node->resource()),
                                       limit)));
            }

            return plan;
        }
    }

} // namespace services::collection::planner::impl
