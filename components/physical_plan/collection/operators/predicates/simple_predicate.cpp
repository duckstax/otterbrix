#include "simple_predicate.hpp"
#include <components/physical_plan/collection/operators/operator.hpp>
#include <regex>

namespace services::collection::operators::predicates {

    simple_predicate::simple_predicate(context_collection_t* context,
                                       std::function<bool(const components::document::document_ptr&,
                                                          const components::document::document_ptr&,
                                                          const components::logical_plan::storage_parameters*)> func)
        : predicate(context)
        , func_(std::move(func)) {}

    simple_predicate::simple_predicate(context_collection_t* context,
                                       std::vector<predicate_ptr>&& nested,
                                       components::expressions::compare_type nested_type)
        : predicate(context)
        , nested_(std::move(nested))
        , nested_type_(nested_type) {}

    bool simple_predicate::check_impl(const components::document::document_ptr& document_left,
                                      const components::document::document_ptr& document_right,
                                      const components::logical_plan::storage_parameters* parameters) {
        switch (nested_type_) {
            case components::expressions::compare_type::union_and:
                for (const auto& predicate : nested_) {
                    if (!predicate->check(document_left, document_right, parameters)) {
                        return false;
                    }
                }
                return true;
            case components::expressions::compare_type::union_or:
                for (const auto& predicate : nested_) {
                    if (predicate->check(document_left, document_right, parameters)) {
                        return true;
                    }
                }
                return false;
            case components::expressions::compare_type::union_not:
                return !nested_.front()->check(document_left, document_right, parameters);
            default:
                break;
        }
        return func_(document_left, document_right, parameters);
    }

    predicate_ptr create_simple_predicate(context_collection_t* context,
                                          const components::expressions::compare_expression_ptr& expr) {
        using components::expressions::compare_type;

        switch (expr->type()) {
            case compare_type::union_and:
            case compare_type::union_or:
            case compare_type::union_not: {
                std::vector<predicate_ptr> nested;
                nested.reserve(expr->children().size());
                for (const auto& nested_expr : expr->children()) {
                    nested.emplace_back(create_simple_predicate(context, nested_expr));
                }
                return {new simple_predicate(context, std::move(nested), expr->type())};
            }
            case compare_type::eq:
                return {new simple_predicate(
                    context,
                    [&expr](const components::document::document_ptr& document_left,
                            const components::document::document_ptr& document_right,
                            const components::logical_plan::storage_parameters* parameters) {
                        if (expr->key_right().is_null()) {
                            auto it = parameters->parameters.find(expr->value());
                            if (it == parameters->parameters.end()) {
                                return false;
                            } else {
                                return document_left->compare(expr->key_left().as_string(), it->second) ==
                                       components::document::compare_t::equals;
                            }
                        } else {
                            auto comp =
                                document_right
                                    ? document_left->compare(expr->key_left().as_string(),
                                                             document_right->get_value(expr->key_right().as_string()))
                                    : document_left->compare(expr->key_left().as_string(),
                                                             document_left->get_value(expr->key_right().as_string()));
                            return comp == components::document::compare_t::equals;
                        }
                    })};
            case compare_type::ne:
                return {new simple_predicate(
                    context,
                    [&expr](const components::document::document_ptr& document_left,
                            const components::document::document_ptr& document_right,
                            const components::logical_plan::storage_parameters* parameters) {
                        if (expr->key_right().is_null()) {
                            auto it = parameters->parameters.find(expr->value());
                            if (it == parameters->parameters.end()) {
                                return false;
                            } else {
                                return document_left->compare(expr->key_left().as_string(), it->second) !=
                                       components::document::compare_t::equals;
                            }
                        } else {
                            auto comp =
                                document_right
                                    ? document_left->compare(expr->key_left().as_string(),
                                                             document_right->get_value(expr->key_right().as_string()))
                                    : document_left->compare(expr->key_left().as_string(),
                                                             document_left->get_value(expr->key_right().as_string()));
                            return comp != components::document::compare_t::equals;
                        }
                    })};
            case compare_type::gt:
                return {new simple_predicate(
                    context,
                    [&expr](const components::document::document_ptr& document_left,
                            const components::document::document_ptr& document_right,
                            const components::logical_plan::storage_parameters* parameters) {
                        if (expr->key_right().is_null()) {
                            auto it = parameters->parameters.find(expr->value());
                            if (it == parameters->parameters.end()) {
                                return false;
                            } else {
                                return document_left->compare(expr->key_left().as_string(), it->second) ==
                                       components::document::compare_t::more;
                            }
                        } else {
                            auto comp =
                                document_right
                                    ? document_left->compare(expr->key_left().as_string(),
                                                             document_right->get_value(expr->key_right().as_string()))
                                    : document_left->compare(expr->key_left().as_string(),
                                                             document_left->get_value(expr->key_right().as_string()));
                            return comp == components::document::compare_t::more;
                        }
                    })};
            case compare_type::gte:
                return {new simple_predicate(
                    context,
                    [&expr](const components::document::document_ptr& document_left,
                            const components::document::document_ptr& document_right,
                            const components::logical_plan::storage_parameters* parameters) {
                        if (expr->key_right().is_null()) {
                            auto it = parameters->parameters.find(expr->value());
                            if (it == parameters->parameters.end()) {
                                return false;
                            } else {
                                auto comp = document_left->compare(expr->key_left().as_string(), it->second);
                                return comp == components::document::compare_t::equals ||
                                       comp == components::document::compare_t::more;
                            }
                        } else {
                            auto comp =
                                document_right
                                    ? document_left->compare(expr->key_left().as_string(),
                                                             document_right->get_value(expr->key_right().as_string()))
                                    : document_left->compare(expr->key_left().as_string(),
                                                             document_left->get_value(expr->key_right().as_string()));
                            return comp == components::document::compare_t::equals ||
                                   comp == components::document::compare_t::more;
                        }
                    })};
            case compare_type::lt:
                return {new simple_predicate(
                    context,
                    [&expr](const components::document::document_ptr& document_left,
                            const components::document::document_ptr& document_right,
                            const components::logical_plan::storage_parameters* parameters) {
                        if (expr->key_right().is_null()) {
                            auto it = parameters->parameters.find(expr->value());
                            if (it == parameters->parameters.end()) {
                                return false;
                            } else {
                                return document_left->compare(expr->key_left().as_string(), it->second) ==
                                       components::document::compare_t::less;
                            }
                        } else {
                            auto comp =
                                document_right
                                    ? document_left->compare(expr->key_left().as_string(),
                                                             document_right->get_value(expr->key_right().as_string()))
                                    : document_left->compare(expr->key_left().as_string(),
                                                             document_left->get_value(expr->key_right().as_string()));
                            return comp == components::document::compare_t::less;
                        }
                    })};
            case compare_type::lte:
                return {new simple_predicate(
                    context,
                    [&expr](const components::document::document_ptr& document_left,
                            const components::document::document_ptr& document_right,
                            const components::logical_plan::storage_parameters* parameters) {
                        if (expr->key_right().is_null()) {
                            auto it = parameters->parameters.find(expr->value());
                            if (it == parameters->parameters.end()) {
                                return false;
                            } else {
                                auto comp = document_left->compare(expr->key_left().as_string(), it->second);
                                return comp == components::document::compare_t::equals ||
                                       comp == components::document::compare_t::less;
                            }
                        } else {
                            auto comp =
                                document_right
                                    ? document_left->compare(expr->key_left().as_string(),
                                                             document_right->get_value(expr->key_right().as_string()))
                                    : document_left->compare(expr->key_left().as_string(),
                                                             document_left->get_value(expr->key_right().as_string()));
                            return comp == components::document::compare_t::equals ||
                                   comp == components::document::compare_t::less;
                        }
                    })};
            case compare_type::regex:
                return {new simple_predicate(
                    context,
                    [&expr](const components::document::document_ptr& document_left,
                            const components::document::document_ptr& document_right,
                            const components::logical_plan::storage_parameters* parameters) {
                        if (expr->key_right().is_null()) {
                            auto it = parameters->parameters.find(expr->value());
                            if (it == parameters->parameters.end()) {
                                return false;
                            } else {
                                return document_left->type_by_key(expr->key_left().as_string()) ==
                                           components::types::logical_type::STRING_LITERAL &&
                                       std::regex_match(document_left->get_string(expr->key_left().as_string()).data(),
                                                        std::regex(fmt::format(".*{}.*", it->second.as_string())));
                            }
                        } else {
                            return document_right
                                       ? document_left->type_by_key(expr->key_left().as_string()) ==
                                                 components::types::logical_type::STRING_LITERAL &&
                                             std::regex_match(
                                                 document_left->get_string(expr->key_left().as_string()).data(),
                                                 std::regex(
                                                     fmt::format(".*{}.*",
                                                                 document_left->get_value(expr->key_right().as_string())
                                                                     .as_string())))
                                       : document_left->type_by_key(expr->key_left().as_string()) ==
                                                 components::types::logical_type::STRING_LITERAL &&
                                             std::regex_match(
                                                 document_left->get_string(expr->key_left().as_string()).data(),
                                                 std::regex(fmt::format(
                                                     ".*{}.*",
                                                     document_right->get_value(expr->key_right().as_string())
                                                         .as_string())));
                        }
                    })};
            case compare_type::all_true:
                return {new simple_predicate(context,
                                             [](const components::document::document_ptr&,
                                                const components::document::document_ptr&,
                                                const components::logical_plan::storage_parameters*) { return true; })};
            case compare_type::all_false:
                return {
                    new simple_predicate(context,
                                         [](const components::document::document_ptr&,
                                            const components::document::document_ptr&,
                                            const components::logical_plan::storage_parameters*) { return false; })};
            default:
                break;
        }
        return {new simple_predicate(context,
                                     [](const components::document::document_ptr&,
                                        const components::document::document_ptr&,
                                        const components::logical_plan::storage_parameters*) { return true; })};
    }

} // namespace services::collection::operators::predicates
