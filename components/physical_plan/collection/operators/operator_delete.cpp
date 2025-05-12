#include "operator_delete.hpp"
#include <services/collection/collection.hpp>

namespace services::collection::operators {

    operator_delete::operator_delete(context_collection_t* context)
        : read_write_operator_t(context, operator_type::remove) {}

    void operator_delete::on_execute_impl(components::pipeline::context_t* pipeline_context) {
        if (left_ && left_->output() && !std::get<std::pmr::vector<::document_ptr>>(left_->output()->data()).empty()) {
            modified_ = base::operators::make_operator_write_data<document_id_t>(context_->resource());
            for (const auto& document : std::get<std::pmr::vector<document_ptr>>(left_->output()->data())) {
                const auto id = components::document::get_document_id(document);
                auto it = context_->document_storage().find(id);
                if (it != context_->document_storage().end()) {
                    context_->document_storage().erase(it);
                    modified_->append(id);
                    context_->index_engine()->delete_document(document, pipeline_context);
                }
            }
        }
    }

} // namespace services::collection::operators
