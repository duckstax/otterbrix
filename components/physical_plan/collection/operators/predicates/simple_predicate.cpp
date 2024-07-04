#include "simple_predicate.hpp"
#include <components/physical_plan/collection/operators/operator.hpp>
#include <regex>

namespace services::collection::operators::predicates {

    simple_predicate::simple_predicate(
        context_collection_t* context,
        std::function<bool(const components::document::document_ptr&, const components::ql::storage_parameters*)> func)
        : predicate(context)
        , func_(std::move(func)) {}

    bool simple_predicate::check_impl(const components::document::document_ptr& document,
                                      const components::ql::storage_parameters* parameters) {
        return func_(document, parameters);
    }

    predicate_ptr create_simple_predicate(context_collection_t* context,
                                          const components::expressions::compare_expression_ptr& expr) {
        using components::expressions::compare_type;

        switch (expr->type()) {
            case compare_type::eq:
                return {new simple_predicate(context,
                                             [&expr](const components::document::document_ptr& document,
                                                     const components::ql::storage_parameters* parameters) {
                                                 auto it = parameters->parameters.find(expr->value());
                                                 if (it == parameters->parameters.end()) {
                                                     return false;
                                                 } else {
                                                     return document->compare(expr->key().as_string(), it->second) ==
                                                            components::document::compare_t::equals;
                                                 }
                                             })};
            case compare_type::ne:
                return {new simple_predicate(context,
                                             [&expr](const components::document::document_ptr& document,
                                                     const components::ql::storage_parameters* parameters) {
                                                 auto it = parameters->parameters.find(expr->value());
                                                 if (it == parameters->parameters.end()) {
                                                     return false;
                                                 } else {
                                                     return document->compare(expr->key().as_string(), it->second) !=
                                                            components::document::compare_t::equals;
                                                 }
                                             })};
            case compare_type::gt:
                return {new simple_predicate(context,
                                             [&expr](const components::document::document_ptr& document,
                                                     const components::ql::storage_parameters* parameters) {
                                                 auto it = parameters->parameters.find(expr->value());
                                                 if (it == parameters->parameters.end()) {
                                                     return false;
                                                 } else {
                                                     return document->compare(expr->key().as_string(), it->second) ==
                                                            components::document::compare_t::more;
                                                 }
                                             })};
            case compare_type::gte:
                return {new simple_predicate(context,
                                             [&expr](const components::document::document_ptr& document,
                                                     const components::ql::storage_parameters* parameters) {
                                                 auto it = parameters->parameters.find(expr->value());
                                                 if (it == parameters->parameters.end()) {
                                                     return false;
                                                 } else {
                                                     auto comp = document->compare(expr->key().as_string(), it->second);
                                                     return comp == components::document::compare_t::equals ||
                                                            comp == components::document::compare_t::more;
                                                 }
                                             })};
            case compare_type::lt:
                return {new simple_predicate(context,
                                             [&expr](const components::document::document_ptr& document,
                                                     const components::ql::storage_parameters* parameters) {
                                                 auto it = parameters->parameters.find(expr->value());
                                                 if (it == parameters->parameters.end()) {
                                                     return false;
                                                 } else {
                                                     return document->compare(expr->key().as_string(), it->second) ==
                                                            components::document::compare_t::less;
                                                 }
                                             })};
            case compare_type::lte:
                return {new simple_predicate(context,
                                             [&expr](const components::document::document_ptr& document,
                                                     const components::ql::storage_parameters* parameters) {
                                                 auto it = parameters->parameters.find(expr->value());
                                                 if (it == parameters->parameters.end()) {
                                                     return false;
                                                 } else {
                                                     auto comp = document->compare(expr->key().as_string(), it->second);
                                                     return comp == components::document::compare_t::equals ||
                                                            comp == components::document::compare_t::less;
                                                 }
                                             })};
            case compare_type::regex:
                return {new simple_predicate(
                    context,
                    [&expr](const components::document::document_ptr& document,
                            const components::ql::storage_parameters* parameters) {
                        auto it = parameters->parameters.find(expr->value());
                        if (it == parameters->parameters.end()) {
                            return false;
                        } else {
                            return document->type_by_key(expr->key().as_string()) ==
                                       components::types::logical_type::STRING_LITERAL &&
                                   std::regex_match(document->get_string(expr->key().as_string()).data(),
                                                    std::regex(fmt::format(".*{}.*", it->second.as_string())));
                        }
                    })};
            case compare_type::all_true:
                return {new simple_predicate(context,
                                             [](const components::document::document_ptr&,
                                                const components::ql::storage_parameters*) { return true; })};
            case compare_type::all_false:
                return {new simple_predicate(context,
                                             [](const components::document::document_ptr&,
                                                const components::ql::storage_parameters*) { return false; })};
            default:
                break;
        }
        return {new simple_predicate(
            context,
            [](const components::document::document_ptr&, const components::ql::storage_parameters*) { return true; })};
    }

} // namespace services::collection::operators::predicates
