#include "full_scan.hpp"
#include <services/collection/operators/predicates/gt.hpp>

namespace services::collection::operators {

    full_scan::full_scan(services::collection::collection_t* collection)
        : scanner(collection) {
    }

    result_find full_scan::scan_impl(components::ql::find_statement& cond) {
        result_find::result_t result;
        auto predicate = create_predicate(cond);
        for (auto& it : collection_->storage()) {
            if (predicate->check(it.second)) {
                result.push_back(document_view_t(it.second));
            }
        }
        return result_find(std::move(result));
    }

    result_find full_scan::scan_one_impl(components::ql::find_statement& cond) {
        auto predicate = create_predicate(cond);
        for (auto& it : collection_->storage()) {
            if (predicate->check(it.second)) {
                return result_find({document_view_t(it.second)});
            }
        }
        return result_find();
    }

} // namespace services::collection::operators
