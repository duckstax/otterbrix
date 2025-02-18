#include "create_plan_match.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/physical_plan/collection/operators/merge/operator_merge.hpp>
#include <components/physical_plan/collection/operators/scan/full_scan.hpp>
#include <components/physical_plan/collection/operators/scan/index_scan.hpp>
#include <components/physical_plan/collection/operators/scan/primary_key_scan.hpp>
#include <components/physical_plan/collection/operators/scan/transfer_scan.hpp>
#include <physical_plan/collection/operators/operator_match.hpp>

namespace services::collection::planner::impl {

    bool is_can_index_find_by_predicate(components::expressions::compare_type compare) {
        using components::expressions::compare_type;
        return compare == compare_type::eq || compare == compare_type::ne || compare == compare_type::gt ||
               compare == compare_type::lt || compare == compare_type::gte || compare == compare_type::lte;
    }

    bool is_can_primary_key_find_by_predicate(components::expressions::compare_type compare) {
        using components::expressions::compare_type;
        return compare == compare_type::eq;
    }

    operators::operator_ptr create_plan_match_(context_collection_t* context_,
                                               const components::expressions::compare_expression_ptr& expr,
                                               components::ql::limit_t limit) {
        if (operators::merge::is_operator_merge(expr)) {
            auto op = operators::merge::create_operator_merge(context_, expr, limit);
            operators::operator_ptr left = nullptr;
            operators::operator_ptr right = nullptr;
            left = create_plan_match_(context_, expr->children().at(0), components::ql::limit_t::unlimit());
            if (expr->children().size() > 1) { //todo: make if size > 2
                right = create_plan_match_(context_, expr->children().at(1), components::ql::limit_t::unlimit());
            }
            op->set_children(std::move(left), std::move(right));
            return op;
        }
        //if (is_can_primary_key_find_by_predicate(expr->type()) && expr->key().as_string() == "_id") {
        //return boost::intrusive_ptr(new operators::primary_key_scan(context_));
        //}
        if (context_) {
            if (is_can_index_find_by_predicate(expr->type()) && search_index(context_->index_engine(), {expr->key()})) {
                return boost::intrusive_ptr(new operators::index_scan(context_, expr, limit));
            }
            auto predicate = operators::predicates::create_predicate(context_, expr);
            return boost::intrusive_ptr(new operators::full_scan(context_, std::move(predicate), limit));
        } else {
            auto predicate = operators::predicates::create_predicate(context_, expr);
            return boost::intrusive_ptr(new operators::operator_match_t(context_, std::move(predicate), limit));
        }
    }

    operators::operator_ptr create_plan_match(const context_storage_t& context,
                                              const components::logical_plan::node_ptr& node,
                                              components::ql::limit_t limit) {
        if (node->expressions().empty()) {
            return boost::intrusive_ptr(new operators::transfer_scan(context.at(node->collection_full_name()), limit));
        } else { //todo: other kinds scan
            auto expr =
                reinterpret_cast<const components::expressions::compare_expression_ptr*>(&node->expressions()[0]);
            return create_plan_match_(context.at(node->collection_full_name()), *expr, limit);
        }
    }

} // namespace services::collection::planner::impl
