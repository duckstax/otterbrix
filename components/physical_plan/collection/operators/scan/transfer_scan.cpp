#include "transfer_scan.hpp"

#include <services/collection/collection.hpp>

namespace components::collection::operators {

    transfer_scan::transfer_scan(services::collection::context_collection_t* context, logical_plan::limit_t limit)
        : read_only_operator_t(context, operator_type::match)
        , limit_(limit) {}

    void transfer_scan::on_execute_impl(pipeline::context_t*) {
        int count = 0;
        if (!limit_.check(count)) {
            return; //limit = 0
        }
        output_ = base::operators::make_operator_data(context_->resource());
        for (auto& it : context_->document_storage()) {
            output_->append(it.second);
            ++count;
            if (!limit_.check(count)) {
                return;
            }
        }
    }

} // namespace components::collection::operators
