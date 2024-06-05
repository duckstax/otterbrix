#include "simple_predicate.hpp"
#include <regex>
#include <components/physical_plan/collection/operators/operator.hpp>

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
                                                 auto it = parameters->find(expr->value());
                                                 if (it == parameters->end()) {
                                                     return false;
                                                 } else {
                                                     auto value = get_value_from_document(document, expr->key());
                                                     return value && value == it->second;
                                                 }
                                             })};
            case compare_type::ne:
                return {new simple_predicate(context,
                                             [&expr](const components::document::document_ptr& document,
                                                     const components::ql::storage_parameters* parameters) {
                                                 auto it = parameters->find(expr->value());
                                                 if (it == parameters->end()) {
                                                     return false;
                                                 } else {
                                                     auto value = get_value_from_document(document, expr->key());
                                                     return value && value != it->second;
                                                 }
                                             })};
            case compare_type::gt:
                return {new simple_predicate(context,
                                             [&expr](const components::document::document_ptr& document,
                                                     const components::ql::storage_parameters* parameters) {
                                                 auto it = parameters->find(expr->value());
                                                 if (it == parameters->end()) {
                                                     return false;
                                                 } else {
                                                     auto value = get_value_from_document(document, expr->key());
                                                     return value && value > it->second;
                                                 }
                                             })};
            case compare_type::gte:
                return {new simple_predicate(context,
                                             [&expr](const components::document::document_ptr& document,
                                                     const components::ql::storage_parameters* parameters) {
                                                 auto it = parameters->find(expr->value());
                                                 if (it == parameters->end()) {
                                                     return false;
                                                 } else {
                                                     auto value = get_value_from_document(document, expr->key());
                                                     return value && value >= it->second;
                                                 }
                                             })};
            case compare_type::lt:
                return {new simple_predicate(context,
                                             [&expr](const components::document::document_ptr& document,
                                                     const components::ql::storage_parameters* parameters) {
                                                 auto it = parameters->find(expr->value());
                                                 if (it == parameters->end()) {
                                                     return false;
                                                 } else {
                                                     auto value = get_value_from_document(document, expr->key());
                                                     return value && value < it->second;
                                                 }
                                             })};
            case compare_type::lte:
                return {new simple_predicate(context,
                                             [&expr](const components::document::document_ptr& document,
                                                     const components::ql::storage_parameters* parameters) {
                                                 auto it = parameters->find(expr->value());
                                                 if (it == parameters->end()) {
                                                     return false;
                                                 } else {
                                                     auto value = get_value_from_document(document, expr->key());
                                                     return value && value <= it->second;
                                                 }
                                             })};
            case compare_type::regex:
                return {new simple_predicate(
                    context,
                    [&expr](const components::document::document_ptr& document,
                            const components::ql::storage_parameters* parameters) {
                        auto it = parameters->find(expr->value());
                        if (it == parameters->end()) {
                            return false;
                        } else {
                            auto value = get_value_from_document(document, expr->key());
                            return value && value->type() == document::impl::value_type::string &&
                                   std::regex_match(value->as_string().data(),
                                                    std::regex(fmt::format(".*{}.*", it->second->as_string())));
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
