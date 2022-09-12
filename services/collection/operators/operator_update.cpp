#include "operator_update.hpp"

namespace services::collection::operators {

    operator_update::operator_update(context_collection_t* context, document_ptr update)
        : read_write_operator_t(context, operator_type::update)
        , update_(std::move(update)) {
    }

    void operator_update::on_execute_impl(operator_data_t* data) {
        if (data) {
            for (auto& document : data->documents()) {
                if (document->update(*update_)) {
                    document->commit();
                }
            }
        }
        //todo: remake indexes
    }

} // namespace services::collection::operators
