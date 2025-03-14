#include "transfer_scan.hpp"

#include <services/collection/collection.hpp>

namespace services::collection::operators {

    transfer_scan::transfer_scan(context_collection_t* context, components::logical_plan::limit_t limit)
        : read_only_operator_t(context, operator_type::match)
        , limit_(limit) {}

    void transfer_scan::on_execute_impl(components::pipeline::context_t*) {
        int count = 0;
        if (!limit_.check(count)) {
            return; //limit = 0
        }
        output_ = make_operator_data(context_->resource());
        for (auto& it : context_->storage()) {
            output_->append(it.second);
            ++count;
            if (!limit_.check(count)) {
                return;
            }
        }
    }

} // namespace services::collection::operators
