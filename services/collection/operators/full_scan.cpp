#include "full_scan.hpp"

#include <services/collection/collection.hpp>

namespace services::collection::operators {

    full_scan::full_scan(context_collection_t* context, predicates::predicate_ptr predicate, predicates::limit_t limit)
        : operator_t(context, operator_type::full_scan)
        , predicate_(std::move(predicate))
        , limit_(limit) {
    }

    void full_scan::on_execute_impl(operator_data_t* data) {
        int count = 0;
        if (!limit_.check(count)) {
            return; //limit = 0
        }
        for (auto& it : context_->storage()) {
            if (predicate_->check(it.second)) {
                data->append(it.second);
                ++count;
                if (!limit_.check(count)) {
                    return;
                }
            }
        }
    }

} // namespace services::collection::operators
