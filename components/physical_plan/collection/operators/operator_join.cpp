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

    bool operator_join_t::check_expressions_(const components::document::document_ptr& left,
                                             const components::document::document_ptr& right) {
        for (const auto& expr : expressions_) {
            if (left->compare(expr->left().expr->key().as_string(), right, expr->right().expr->key().as_string()) !=
                components::document::compare_t::equals) {
                return false;
            }
        }
        return true;
    }

    void operator_join_t::on_execute_impl([[maybe_unused]] components::pipeline::context_t* context) {
        if (!left_ || !right_) {
            return;
        }
        if (left_->output() && right_->output()) {
            output_ = make_operator_data(left_->output()->resource());

            if (context_) {
                // With introduction of raw_data without context, log is not guaranteed to be here
                // TODO: acquire log from different means
                trace(context_->log(), "operator_join::left_size(): {}", left_->output()->documents().size());
                trace(context_->log(), "operator_join::right_size(): {}", right_->output()->documents().size());
            }

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

            if (context_) {
                // Same reason as above
                trace(context_->log(), "operator_join::result_size(): {}", output_->documents().size());
            }
        }
    }

    void operator_join_t::inner_join_() {
        for (auto doc_left : left_->output()->documents()) {
            for (auto doc_right : right_->output()->documents()) {
                if (check_expressions_(doc_left, doc_right)) {
                    output_->append(document_t::merge(doc_left, doc_right, left_->output()->resource()));
                }
            }
        }
    }

    void operator_join_t::outer_full_join_() {
        auto empty_left = components::document::make_document(left_->output()->resource());
        auto empty_right = components::document::make_document(left_->output()->resource());
        if (!left_->output()->documents().empty()) {
            auto doc = left_->output()->documents().front();
            auto fields = doc->json_trie()->as_object();
            for (auto it_field = fields->begin(); it_field != fields->end(); ++it_field) {
                empty_left->set(it_field->first->get_mut()->get_string(), nullptr);
            }
        }
        if (!right_->output()->documents().empty()) {
            auto doc = right_->output()->documents().front();
            auto fields = doc->json_trie()->as_object();
            for (auto it_field = fields->begin(); it_field != fields->end(); ++it_field) {
                empty_right->set(it_field->first->get_mut()->get_string(), nullptr);
            }
        }

        std::vector<bool> visited_right(right_->output()->documents().size(), false);
        for (auto doc_left : left_->output()->documents()) {
            bool visited_left = false;
            size_t right_index = 0;
            for (auto doc_right : right_->output()->documents()) {
                if (check_expressions_(doc_left, doc_right)) {
                    visited_left = true;
                    visited_right[right_index] = true;
                    output_->append(std::move(document_t::merge(doc_left, doc_right, left_->output()->resource())));
                }
                right_index++;
            }
            if (!visited_left) {
                output_->append(std::move(document_t::merge(doc_left, empty_right, left_->output()->resource())));
            }
        }
        for (size_t i = 0; i < visited_right.size(); ++i) {
            if (visited_right[i]) {
                continue;
            }
            output_->append(std::move(
                document_t::merge(empty_left, right_->output()->documents().at(i), left_->output()->resource())));
        }
    }

    void operator_join_t::outer_left_join_() {
        auto empty_right = components::document::make_document(left_->output()->resource());
        if (!right_->output()->documents().empty()) {
            auto doc = right_->output()->documents().front();
            auto fields = doc->json_trie()->as_object();
            for (auto it_field = fields->begin(); it_field != fields->end(); ++it_field) {
                empty_right->set(it_field->first->get_mut()->get_string(), nullptr);
            }
        }

        for (auto doc_left : left_->output()->documents()) {
            bool visited_left = false;
            for (auto doc_right : right_->output()->documents()) {
                if (check_expressions_(doc_left, doc_right)) {
                    visited_left = true;
                    output_->append(std::move(document_t::merge(doc_left, doc_right, left_->output()->resource())));
                }
            }
            if (!visited_left) {
                output_->append(std::move(document_t::merge(doc_left, empty_right, left_->output()->resource())));
            }
        }
    }

    void operator_join_t::outer_right_join_() {
        auto empty_left = components::document::make_document(left_->output()->resource());
        if (!left_->output()->documents().empty()) {
            auto doc = left_->output()->documents().front();
            auto fields = doc->json_trie()->as_object();
            for (auto it_field = fields->begin(); it_field != fields->end(); ++it_field) {
                empty_left->set(it_field->first->get_mut()->get_string(), nullptr);
            }
        }

        for (auto doc_right : right_->output()->documents()) {
            bool visited_right = false;
            for (auto doc_left : left_->output()->documents()) {
                if (check_expressions_(doc_left, doc_right)) {
                    visited_right = true;
                    output_->append(std::move(document_t::merge(doc_left, doc_right, left_->output()->resource())));
                }
            }
            if (!visited_right) {
                output_->append(std::move(document_t::merge(empty_left, doc_right, left_->output()->resource())));
            }
        }
    }

    void operator_join_t::cross_join_() {
        for (auto doc_left : left_->output()->documents()) {
            for (auto doc_right : right_->output()->documents()) {
                output_->append(std::move(document_t::merge(doc_left, doc_right, left_->output()->resource())));
            }
        }
    }

} // namespace services::collection::operators
