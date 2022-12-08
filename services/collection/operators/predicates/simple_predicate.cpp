#include "simple_predicate.hpp"
#include <regex>

namespace services::collection::operators::predicates {

    simple_predicate::simple_predicate(context_collection_t* context, std::function<bool(const components::document::document_ptr&)> func)
        : predicate(context)
        , func_(std::move(func)) {}

    bool simple_predicate::check_impl(const components::document::document_ptr& document) {
        return func_(document);
    }

    predicate_ptr create_simple_predicate(context_collection_t* context, const components::ql::expr_ptr& expr) {
        using components::ql::condition_type;

        switch (expr->type_) {
            case condition_type::eq:
                return std::make_unique<simple_predicate>(context, [&expr](const components::document::document_ptr& document) {
                    auto value = get_value_from_document(document, expr->key_);
                    return value && value == expr->value_;
                });
            case condition_type::ne:
                return std::make_unique<simple_predicate>(context, [&expr](const components::document::document_ptr& document) {
                    auto value = get_value_from_document(document, expr->key_);
                    return value && value != expr->value_;
                });
            case condition_type::gt:
                return std::make_unique<simple_predicate>(context, [&expr](const components::document::document_ptr& document) {
                    auto value = get_value_from_document(document, expr->key_);
                    return value && value > expr->value_;
                });
            case condition_type::gte:
                return std::make_unique<simple_predicate>(context, [&expr](const components::document::document_ptr& document) {
                    auto value = get_value_from_document(document, expr->key_);
                    return value && value >= expr->value_;
                });
            case condition_type::lt:
                return std::make_unique<simple_predicate>(context, [&expr](const components::document::document_ptr& document) {
                    auto value = get_value_from_document(document, expr->key_);
                    return value && value < expr->value_;
                });
            case condition_type::lte:
                return std::make_unique<simple_predicate>(context, [&expr](const components::document::document_ptr& document) {
                    auto value = get_value_from_document(document, expr->key_);
                    return value && value <= expr->value_;
                });
            case condition_type::regex:
                return std::make_unique<simple_predicate>(context, [&expr](const components::document::document_ptr& document) {
                    auto value = get_value_from_document(document, expr->key_);
                   return value && value->type() == document::impl::value_type::string &&
                           std::regex_match(get_value_from_document(document, expr->key_)->to_string().data(),
                                            std::regex(fmt::format(".*{}.*",expr->value_->to_string())));
                });
            default:
                break;
        }
        return std::make_unique<simple_predicate>(context, [](const components::document::document_ptr&) {
            return true;
        });
    }

    predicate_ptr create_simple_predicate(context_collection_t* context, const components::ql::find_statement_ptr& cond) {
        return create_simple_predicate(context, cond->condition_);
    }

} // namespace services::operators::predicates
