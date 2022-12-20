#include "scan.hpp"
#include "full_scan.hpp"
#include <services/collection/operators/merge/operator_merge.hpp>

namespace services::collection::operators {

    operator_ptr create_searcher(context_collection_t* context, const components::expressions::compare_expression_ptr& expr, predicates::limit_t limit) {
        //todo: choice searcher
        if (merge::is_operator_merge(expr)) {
            auto op = merge::create_operator_merge(context, expr, limit);
            operator_ptr left = nullptr;
            operator_ptr right = nullptr;
            left = create_searcher(context, expr->children().at(0), predicates::limit_t::unlimit());
            if (expr->children().size() > 1) { //todo: make if size > 2
                right = create_searcher(context, expr->children().at(1), predicates::limit_t::unlimit());
            }
            op->set_children(std::move(left), std::move(right));
            return op;
        }
        return std::make_unique<full_scan>(context, predicates::create_predicate(context, expr), limit);
    }

} // namespace services::collection::operators
