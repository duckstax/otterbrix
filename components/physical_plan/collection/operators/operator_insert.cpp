#include "operator_insert.hpp"
#include <services/collection/collection.hpp>

namespace services::collection::operators {

    operator_insert::operator_insert(context_collection_t* context)
        : read_write_operator_t(context, operator_type::insert) {}

    void operator_insert::on_execute_impl(components::pipeline::context_t* pipeline_context) {
        if (left_ && left_->output()) {
            modified_ = make_operator_write_data(context_->resource());
            output_ = make_operator_data(context_->resource());
            for (const auto& document : left_->output()->documents()) {
                auto id = get_document_id(document);
                context_->storage().insert_or_assign(id, document);
                context_->index_engine()->insert_document(document, pipeline_context);
                output_->append(document);
                modified_->append(std::move(id));
            }
        }
    }

} // namespace services::collection::operators
