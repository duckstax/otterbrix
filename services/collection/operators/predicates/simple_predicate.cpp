#include "simple_predicate.hpp"
#include <regex>

namespace services::collection::operators::predicates {

    simple_predicate::simple_predicate(context_collection_t* context, std::function<bool(const components::document::document_ptr&)> func)
        : predicate(context)
        , func_(std::move(func)) {}

    bool simple_predicate::check_impl(const components::document::document_ptr& document) {
        return func_(document);
    }

    predicate_ptr create_simple_predicate(context_collection_t* context, const components::expressions::compare_expression_ptr& expr) {
        using components::expressions::compare_type;

        /*
        switch (expr->type()) {
            case compare_type::eq:
                return std::make_unique<simple_predicate>(context, [&expr](const components::document::document_ptr& document) {
                    auto value = get_value_from_document(document, expr->key_);
                    return value && value == expr->value_;
                });
            case compare_type::ne:
                return std::make_unique<simple_predicate>(context, [&expr](const components::document::document_ptr& document) {
                    auto value = get_value_from_document(document, expr->key_);
                    return value && value != expr->value_;
                });
            case compare_type::gt:
                return std::make_unique<simple_predicate>(context, [&expr](const components::document::document_ptr& document) {
                    auto value = get_value_from_document(document, expr->key_);
                    return value && value > expr->value_;
                });
            case compare_type::gte:
                return std::make_unique<simple_predicate>(context, [&expr](const components::document::document_ptr& document) {
                    auto value = get_value_from_document(document, expr->key_);
                    return value && value >= expr->value_;
                });
            case compare_type::lt:
                return std::make_unique<simple_predicate>(context, [&expr](const components::document::document_ptr& document) {
                    auto value = get_value_from_document(document, expr->key_);
                    return value && value < expr->value_;
                });
            case compare_type::lte:
                return std::make_unique<simple_predicate>(context, [&expr](const components::document::document_ptr& document) {
                    auto value = get_value_from_document(document, expr->key_);
                    return value && value <= expr->value_;
                });
            case compare_type::regex:
                return std::make_unique<simple_predicate>(context, [&expr](const components::document::document_ptr& document) {
                    auto value = get_value_from_document(document, expr->key_);
                   return value && value->type() == document::impl::value_type::string &&
                           std::regex_match(to_string(*get_value_from_document(document, expr->key_)).data(),
                                            std::regex(fmt::format(".*{}.*", to_string(*expr->value_))));
                });
            default:
                break;
        }
        */
        return std::make_unique<simple_predicate>(context, [](const components::document::document_ptr&) {
            return true;
        });
    }

} // namespace services::operators::predicates
