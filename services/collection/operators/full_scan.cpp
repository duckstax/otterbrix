#include "full_scan.hpp"
#include <services/collection/operators/predicates/gt.hpp>

namespace services::collection::operators {

    full_scan::full_scan(services::collection::collection_t* collection)
        : operator_t(collection) {
    }

    void full_scan::on_execute_impl(predicate_ptr predicate,limit_t limit,components::cursor::sub_cursor_t* cursor) {
        for (auto& it : collection_->storage()) {
            if (predicate->check(it.second) && limit) {
                cursor->append({document_view_t(it.second)});
            }
        }
    }

} // namespace services::collection::operators
