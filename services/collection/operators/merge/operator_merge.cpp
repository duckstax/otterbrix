#include "operator_merge.hpp"
#include "operator_and.hpp"
#include "operator_or.hpp"
#include "operator_not.hpp"
#include <services/collection/operators/predicates/predicate.hpp>

namespace services::collection::operators::merge {

    operator_merge_t::operator_merge_t(context_collection_t* context)
        : read_only_operator_t(context, operator_type::match) {
    }

    void operator_merge_t::on_execute_impl(planner::transaction_context_t* transaction_context) {
        on_merge_impl(transaction_context);
    }

    bool is_operator_merge(const components::ql::expr_ptr& expr) {
        return expr->is_union();
    }

    operator_merge_ptr create_operator_merge(context_collection_t* context, const components::ql::expr_ptr& expr) {
        using components::ql::condition_type;
        switch (expr->type_) {
            case condition_type::union_and:
                return std::make_unique<operator_and_t>(context);
            case condition_type::union_or:
                return std::make_unique<operator_or_t>(context);
            case condition_type::union_not:
                return std::make_unique<operator_not_t>(context);
            default:
                break;
        }
        return nullptr;
    }

    operator_merge_ptr create_operator_merge(context_collection_t* context, const components::ql::find_statement_ptr& cond) {
        return create_operator_merge(context, cond->condition_);
    }

} // namespace services::collection::operators::merge