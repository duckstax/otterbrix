#include "operator_insert.hpp"
#include <services/collection/collection.hpp>

namespace services::collection::operators {

    operator_insert::operator_insert(
        context_collection_t* context,
        std::pmr::vector<std::pair<components::expressions::key_t, components::expressions::key_t>> key_translation)
        : read_write_operator_t(context, operator_type::insert)
        , key_translation_(std::move(key_translation)) {}

    void operator_insert::on_execute_impl(components::pipeline::context_t* pipeline_context) {
        if (left_ && left_->output()) {
            modified_ = make_operator_write_data(context_->resource());
            output_ = make_operator_data(context_->resource());
            bool simple_insert = true;
            for (const auto& k_pair : key_translation_) {
                if (k_pair.first != k_pair.second) {
                    simple_insert = false;
                    break;
                }
            }
            for (const auto& document : left_->output()->documents()) {
                auto id = get_document_id(document);
                if (simple_insert) {
                    context_->storage().insert_or_assign(id, document);
                    context_->index_engine()->insert_document(document, pipeline_context);
                    output_->append(document);
                } else {
                    auto res_doc = components::document::make_document(context_->resource());
                    res_doc->set("/_id", id.to_string());
                    for (const auto& k_pair : key_translation_) {
                        res_doc->set(k_pair.first.as_string(), document, k_pair.second.as_string());
                    }
                    context_->storage().insert_or_assign(id, res_doc);
                    context_->index_engine()->insert_document(res_doc, pipeline_context);
                    output_->append(res_doc);
                }
                modified_->append(std::move(id));
            }
        }
    }

} // namespace services::collection::operators
