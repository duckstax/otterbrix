#include "simple_predicate.hpp"

namespace services::collection::operators::predicates {

    simple_predicate::simple_predicate(context_collection_t* context, std::function<bool(const components::document::document_ptr&)> func)
        : predicate(context)
        , func_(std::move(func)) {}

    bool simple_predicate::check_impl(const components::document::document_ptr& document) {
        return func_(document);
    }

    predicate_ptr create_simple_predicate(context_collection_t* context, components::ql::find_statement& cond) {
        using components::ql::condition_type;

        switch (cond.condition_->type_) {
            case condition_type::eq:
                return std::make_unique<simple_predicate>(context, [&cond](const components::document::document_ptr& document) {
                    return get_value_from_document(document, cond.condition_->key_) == cond.condition_->value_;
                });
            case condition_type::ne:
                return std::make_unique<simple_predicate>(context, [&cond](const components::document::document_ptr& document) {
                    return get_value_from_document(document, cond.condition_->key_) != cond.condition_->value_;
                });
            case condition_type::gt:
                return std::make_unique<simple_predicate>(context, [&cond](const components::document::document_ptr& document) {
                    return get_value_from_document(document, cond.condition_->key_) > cond.condition_->value_;
                });
            case condition_type::gte:
                return std::make_unique<simple_predicate>(context, [&cond](const components::document::document_ptr& document) {
                    return get_value_from_document(document, cond.condition_->key_) >= cond.condition_->value_;
                });
            case condition_type::lt:
                return std::make_unique<simple_predicate>(context, [&cond](const components::document::document_ptr& document) {
                    return get_value_from_document(document, cond.condition_->key_) < cond.condition_->value_;
                });
            case condition_type::lte:
                return std::make_unique<simple_predicate>(context, [&cond](const components::document::document_ptr& document) {
                    return get_value_from_document(document, cond.condition_->key_) <= cond.condition_->value_;
                });
            default:
                break;
        }
        return nullptr;
    }

} // namespace services::operators::predicates
