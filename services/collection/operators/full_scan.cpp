#include "full_scan.hpp"
#include <services/collection/operators/predicates/gt.hpp>

#include <services/collection/collection.hpp>

namespace services::collection::operators {

    full_scan::full_scan(context_collection_t* context)
        : operator_t(context, operator_type::full_scan) {
    }

    void full_scan::on_execute_impl(const predicate_ptr& predicate, predicates::limit_t limit, components::cursor::sub_cursor_t* cursor) {
        int count = 0;
        for (auto& it : context_->storage()) {
            if (predicate->check(it.second) && limit.limit_ <= count) {
                cursor->append({document_view_t(it.second)});
                ++count;
            }
        }
    }

} // namespace services::collection::operators
