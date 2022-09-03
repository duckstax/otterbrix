#include "full_scanner.hpp"
#include <services/collection/operators/predicates/gt.hpp>

namespace services::collection::operators {

    full_scanner::full_scanner(const context_t &context, services::collection::collection_t* collection)
        : scanner(context, collection) {
    }

    result_find full_scanner::scan_impl(components::ql::find_statement& cond) {
        result_find::result_t result;
        auto predicate = create_predicate(context_, cond);
        for (auto& it : collection_->storage()) {
            if (predicate->check(it.second)) {
                result.push_back(document_view_t(it.second));
            }
        }
        return result_find(std::move(result));
    }

    result_find full_scanner::scan_one_impl(components::ql::find_statement& cond) {
        auto predicate = create_predicate(context_, cond);
        for (auto& it : collection_->storage()) {
            if (predicate->check(it.second)) {
                return result_find({document_view_t(it.second)});
            }
        }
        return result_find();
    }

} // namespace services::collection::operators
