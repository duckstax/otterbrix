#include "operator_delete.hpp"

namespace services::collection::operators {

    operator_delete::operator_delete(context_collection_t* context)
        : operator_t(context, operator_type::remove) {
    }

    void operator_delete::on_execute_impl(operator_data_t* data) {
        if (data) {
            for (const auto& document : data->documents()) {
                context_->storage().erase(context_->storage().find(get_document_id(document)));
            }
        }
    }

} // namespace services::collection::operators
