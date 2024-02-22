#include "operator_join.hpp"

#include <services/collection/collection.hpp>

namespace services::collection::operators {

    operator_join_t::operator_join_t(context_collection_t* context,
                                     const components::expressions::join_expression_t* expr)
        : read_only_operator_t(context, operator_type::join)
        , expr_(expr) {}

    void operator_join_t::on_execute_impl(components::pipeline::context_t* context) {
        if (!left_ || !right_) {
            return;
        }
        if (left_->output() && right_->output()) {
            output_ = make_operator_data(context_->resource());
            for (const auto doc_left : left_->output()->documents()) {
                document_view_t left_view(doc_left);
                for (const auto doc_right : right_->output()->documents()) {
                    document_view_t right_view(doc_right);
                    if (left_view.get(expr_->left().expr->key().as_string())
                            ->is_equal(right_view.get(expr_->right().expr->key().as_string()))) {
                        trace(context_->log(), "operator_join::output_size: {}", output_->size());

                        auto combined_doc = components::document::make_document();
                        {
                            auto fields = left_view.as_dict();
                            for (auto it_field = fields->begin(); it_field; ++it_field) {
                                combined_doc->set(static_cast<std::string>(it_field.key()->as_string()),
                                                  it_field.value());
                            }
                        }
                        {
                            auto fields = right_view.as_dict();
                            for (auto it_field = fields->begin(); it_field; ++it_field) {
                                combined_doc->set(static_cast<std::string>(it_field.key()->as_string()),
                                                  it_field.value());
                            }
                        }
                        output_->append(std::move(combined_doc));

                        /* does not work */

                        /*
                        auto combined_doc = components::document::make_document();
                        combined_doc->update(doc_left.get());
                        combined_doc->update(doc_right.get());
                        output_->append(std::move(combined_doc));
                        */
                    }
                }
            }
        }
    }

} // namespace services::collection::operators
