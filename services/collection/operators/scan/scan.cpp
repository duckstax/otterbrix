#include "scan.hpp"
#include "full_scan.hpp"
#include <services/collection/operators/merge/operator_merge.hpp>

namespace services::collection::operators {

    operator_ptr create_searcher(context_collection_t* context, const components::ql::expr_ptr& statement, predicates::limit_t limit) {
        //todo: choice searcher
        if (merge::is_operator_merge(statement)) {
            auto op = merge::create_operator_merge(context, statement, limit);
            if (!statement->sub_conditions_.empty()) {
                operator_ptr left = nullptr;
                operator_ptr right = nullptr;
                left = create_searcher(context, statement->sub_conditions_.at(0), predicates::limit_t::unlimit()); //todo: limit not valid
                if (statement->sub_conditions_.size() > 1) {
                    right = create_searcher(context, statement->sub_conditions_.at(1), predicates::limit_t::unlimit());
                }
                op->set_children(std::move(left), std::move(right));
            }
            return op;
        }
        return std::make_unique<full_scan>(context, predicates::create_predicate(context, statement), limit);
    }

} // namespace services::collection::operators
