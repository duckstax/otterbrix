#include "create_plan_insert.hpp"
#include <components/logical_plan/node_insert.hpp>
#include <services/collection/operators/operator_insert.hpp>
#include <services/collection/operators/scan/primary_key_scan.hpp>

namespace services::collection::planner::impl {

    operators::operator_ptr create_plan_insert(const context_storage_t& context,
                                               const components::logical_plan::node_ptr& node) {
        const auto* insert = static_cast<const components::logical_plan::node_insert_t*>(node.get());
        auto plan = boost::intrusive_ptr(
            new operators::operator_insert(context.at(node->collection_full_name()), insert->documents()));

        auto checker = boost::intrusive_ptr(new operators::primary_key_scan(context.at(node->collection_full_name())));
        for (const auto& document : insert->documents()) {
            checker->append(get_document_id(document));
        }
        plan->set_children(std::move(checker));

        return plan;
    }

} // namespace services::collection::planner::impl
