#include "operator_join.hpp"

#include <services/collection/collection.hpp>
#include <vector>

namespace services::collection::operators {

    operator_join_t::operator_join_t(context_collection_t* context,
                                     type join_type,
                                     std::pmr::vector<components::expressions::join_expression_ptr>&& expressions)
        : read_only_operator_t(context, operator_type::join)
        , join_type_(join_type)
        , expressions_(std::move(expressions)) {}

    bool operator_join_t::check_expressions_(document_view_t left, document_view_t right) {
        for (const auto& expr : expressions_) {
            if (!left.get(expr->left().expr->key().as_string())
                     ->is_equal(right.get(expr->right().expr->key().as_string()))) {
                return false;
            }
        }
        return true;
    }

    void operator_join_t::on_execute_impl(components::pipeline::context_t* context) {
        if (!left_ || !right_) {
            return;
        }
        if (left_->output() && right_->output()) {
            output_ = make_operator_data(context_->resource());

            trace(context_->log(), "operator_join::left_size(): {}", left_->output()->documents().size());
            trace(context_->log(), "operator_join::right_size(): {}", right_->output()->documents().size());

            switch (join_type_) {
                case type::inner:
                    inner_join_();
                    break;
                case type::full:
                    outer_full_join_();
                    break;
                case type::left:
                    outer_left_join_();
                    break;
                case type::right:
                    outer_right_join_();
                    break;
                case type::cross:
                    cross_join_();
                    break;
                default:
                    break;
            }

            trace(context_->log(), "operator_join::result_size(): {}", output_->documents().size());
        }
    }

    void operator_join_t::inner_join_() {
        for (const auto doc_left : left_->output()->documents()) {
            document_view_t left_view(doc_left);
            for (const auto doc_right : right_->output()->documents()) {
                document_view_t right_view(doc_right);
                if (check_expressions_(document_view_t(doc_left), document_view_t(doc_right))) {
                    output_->append(
                        std::move(components::document::combine_documents(doc_left, doc_right)));
                }
            }
        }
    }

    void operator_join_t::outer_full_join_() {
        auto empty_left = components::document::make_document();
        auto empty_right = components::document::make_document();
        if (!left_->output()->documents().empty()) {
            document_view_t view(left_->output()->documents().front());
            auto fields = view.as_dict();
            for (auto it_field = fields->begin(); it_field; ++it_field) {
                empty_left->set(static_cast<std::string>(it_field.key()->as_string()), nullptr);
            }
        }
        if (!right_->output()->documents().empty()) {
            document_view_t view(right_->output()->documents().front());
            auto fields = view.as_dict();
            for (auto it_field = fields->begin(); it_field; ++it_field) {
                empty_right->set(static_cast<std::string>(it_field.key()->as_string()), nullptr);
            }
        }

        std::vector<bool> visited_right(right_->output()->documents().size(), false);
        for (const auto doc_left : left_->output()->documents()) {
            document_view_t left_view(doc_left);
            bool visited_left = false;
            size_t right_index = 0;
            for (const auto doc_right : right_->output()->documents()) {
                document_view_t right_view(doc_right);

                if (check_expressions_(document_view_t(doc_left), document_view_t(doc_right))) {
                    visited_left = true;
                    visited_right[right_index] = true;
                    output_->append(
                        std::move(components::document::combine_documents(doc_left, doc_right)));
                }
                right_index++;
            }
            if (!visited_left) {
                output_->append(std::move(components::document::combine_documents(doc_left, empty_right)));
            }
        }
        for (size_t i = 0; i < visited_right.size(); ++i) {
            if (visited_right[i]) {
                continue;
            }
            output_->append(std::move(
                components::document::combine_documents(empty_left, right_->output()->documents().at(i))));
        }
    }

    void operator_join_t::outer_left_join_() {
        auto empty_right = components::document::make_document();
        if (!right_->output()->documents().empty()) {
            document_view_t view(right_->output()->documents().front());
            auto fields = view.as_dict();
            for (auto it_field = fields->begin(); it_field; ++it_field) {
                empty_right->set(static_cast<std::string>(it_field.key()->as_string()), nullptr);
            }
        }

        for (const auto doc_left : left_->output()->documents()) {
            document_view_t left_view(doc_left);
            bool visited_left = false;
            for (const auto doc_right : right_->output()->documents()) {
                document_view_t right_view(doc_right);

                if (check_expressions_(document_view_t(doc_left), document_view_t(doc_right))) {
                    visited_left = true;
                    output_->append(
                        std::move(components::document::combine_documents(doc_left, doc_right)));
                }
            }
            if (!visited_left) {
                output_->append(std::move(components::document::combine_documents(doc_left, empty_right)));
            }
        }
    }

    void operator_join_t::outer_right_join_() {
        auto empty_left = components::document::make_document();
        if (!left_->output()->documents().empty()) {
            document_view_t view(left_->output()->documents().front());
            auto fields = view.as_dict();
            for (auto it_field = fields->begin(); it_field; ++it_field) {
                empty_left->set(static_cast<std::string>(it_field.key()->as_string()), nullptr);
            }
        }

        for (const auto doc_right : right_->output()->documents()) {
            document_view_t right_view(doc_right);
            bool visited_right = false;
            for (const auto doc_left : left_->output()->documents()) {
                document_view_t left_view(doc_left);

                if (check_expressions_(document_view_t(doc_left), document_view_t(doc_right))) {
                    visited_right = true;
                    output_->append(
                        std::move(components::document::combine_documents(doc_left, doc_right)));
                }
            }
            if (!visited_right) {
                output_->append(std::move(components::document::combine_documents(empty_left, doc_right)));
            }
        }
    }

    void operator_join_t::cross_join_() {
        for (const auto doc_left : left_->output()->documents()) {
            document_view_t left_view(doc_left);
            for (const auto doc_right : right_->output()->documents()) {
                document_view_t right_view(doc_right);
                output_->append(std::move(components::document::combine_documents(doc_left, doc_right)));
            }
        }
    }

} // namespace services::collection::operators
