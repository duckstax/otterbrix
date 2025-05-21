#include "operator_delete.hpp"
#include <services/collection/collection.hpp>

namespace services::collection::operators {

    operator_delete::operator_delete(context_collection_t* context, predicates::predicate_ptr&& match_predicate)
        : read_write_operator_t(context, operator_type::remove)
        , match_predicate_(match_predicate) {}

    void operator_delete::on_execute_impl(components::pipeline::context_t* pipeline_context) {
        // TODO: worth to create separate update_join operator or mutable_join with callback
        if (left_ && left_->output() && right_ && right_->output()) {
            for (auto& doc_left : left_->output()->documents()) {
                for (auto& doc_right : right_->output()->documents()) {
                    if (match_predicate_->check(doc_left, doc_right, &pipeline_context->parameters)) {
                        const auto id = get_document_id(doc_left);
                        auto it = context_->storage().find(id);
                        if (it != context_->storage().end()) {
                            context_->storage().erase(it);
                            modified_->append(id);
                            context_->index_engine()->delete_document(doc_left, pipeline_context);
                        }
                        break;
                    }
                }
            }
        } else if (left_ && left_->output()) {
            modified_ = make_operator_write_data(context_->resource());
            for (const auto& document : left_->output()->documents()) {
                const auto id = get_document_id(document);
                auto it = context_->storage().find(id);
                if (it != context_->storage().end()) {
                    context_->storage().erase(it);
                    modified_->append(id);
                    context_->index_engine()->delete_document(document, pipeline_context);
                }
            }
        }
    }

} // namespace services::collection::operators
