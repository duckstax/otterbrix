#include "parser_where.hpp"
#include <components/expressions/expression.hpp>
#include <components/expressions/join_expression.hpp>
#include <components/sql/parser/base/parser_mask.hpp>

namespace components::sql::impl {

    namespace {

        static const std::vector<mask_element_t> where_stop_words{mask_element_t{token_type::bare_word, "group"},
                                                                  mask_element_t{token_type::bare_word, "order"},
                                                                  mask_element_t{token_type::bare_word, "limit"}};

        static const std::vector<mask_element_t> join_stop_words{mask_element_t{token_type::bare_word, "join"},
                                                                 mask_element_t{token_type::bare_word, "where"},
                                                                 mask_element_t{token_type::bare_word, "group"},
                                                                 mask_element_t{token_type::bare_word, "order"},
                                                                 mask_element_t{token_type::bare_word, "limit"}};

        static const std::vector<mask_element_t> join_on_stop_words{mask_element_t{token_type::bare_word, "and"},
                                                                    mask_element_t{token_type::bare_word, "or"}};

        static const mask_element_t mask_not{token_type::bare_word, "not"};
        static const mask_element_t mask_and{token_type::bare_word, "and"};
        static const mask_element_t mask_or{token_type::bare_word, "or"};
        static const mask_element_t mask_regexp{token_type::bare_word, "regexp"};

        inline bool is_token_where_end(const token_t& token) {
            return std::find_if(where_stop_words.begin(), where_stop_words.end(), [&](const mask_element_t& elem) {
                       return elem == token;
                   }) != where_stop_words.end();
        }

        inline bool is_token_join_end(const token_t& token) {
            return std::find_if(join_stop_words.begin(), join_stop_words.end(), [&](const mask_element_t& elem) {
                       return elem == token;
                   }) != join_stop_words.end();
        }

        inline bool is_token_join_on_end(const token_t& token) {
            return std::find_if(join_on_stop_words.begin(), join_on_stop_words.end(), [&](const mask_element_t& elem) {
                       return elem == token;
                   }) != join_on_stop_words.end();
        }

        inline bool is_token_operand(const token_t& token) {
            return is_token_field_name(token) || is_token_field_value(token);
        }

        inline bool is_token_operator(const token_t& token) {
            return token.type == token_type::equals || token.type == token_type::not_equals ||
                   token.type == token_type::less || token.type == token_type::less_or_equals ||
                   token.type == token_type::greater || token.type == token_type::greater_or_equals ||
                   mask_regexp == token;
        }

        inline expressions::compare_type get_compare_expression(const token_t& token) {
            switch (token.type) {
                case token_type::equals:
                    return expressions::compare_type::eq;
                case token_type::not_equals:
                    return expressions::compare_type::ne;
                case token_type::less:
                    return expressions::compare_type::lt;
                case token_type::less_or_equals:
                    return expressions::compare_type::lte;
                case token_type::greater:
                    return expressions::compare_type::gt;
                case token_type::greater_or_equals:
                    return expressions::compare_type::gte;
                default:
                    break;
            }
            if (mask_regexp == token) {
                return expressions::compare_type::regex;
            }
            return expressions::compare_type::invalid;
        }

        struct parse_where_result {
            parser_result error;
            expressions::compare_expression_ptr expr;
        };

        parse_where_result parse_where(std::pmr::memory_resource* resource,
                                       std::pmr::vector<token_t>::const_iterator begin,
                                       std::pmr::vector<token_t>::const_iterator end,
                                       ql::ql_param_statement_t& statement) {
            if (begin == end || begin + 1 == end || begin + 2 == end) {
                return {parser_result{parse_error::syntax_error, *begin, "not valid where condition"}, nullptr};
            }
            int bracket_open = 0;
            for (auto it = begin; it != end; ++it) {
                if (it->type == token_type::bracket_round_open) {
                    ++bracket_open;
                }
                if (it->type == token_type::bracket_round_close) {
                    --bracket_open;
                    if (bracket_open < 0) {
                        return {
                            parser_result{parse_error::not_exists_open_round_bracket, *it, "not valid where condition"},
                            nullptr};
                    }
                }
                if (bracket_open == 0 && (mask_and == *it || mask_or == *it)) {
                    auto left = parse_where(resource, begin, it, statement);
                    if (left.error.is_error()) {
                        return left;
                    }
                    auto right = parse_where(resource, it + 1, end, statement);
                    if (right.error.is_error()) {
                        return right;
                    }
                    auto expr = expressions::make_compare_union_expression(
                        resource,
                        mask_and == *it ? expressions::compare_type::union_and : expressions::compare_type::union_or);
                    auto append = [&expr](expressions::compare_expression_ptr& e) {
                        if (expr->type() == e->type()) {
                            for (auto& child : e->children()) {
                                expr->append_child(child);
                            }
                        } else {
                            expr->append_child(e);
                        }
                    };
                    append(left.expr);
                    append(right.expr);
                    return {parser_result{true}, expr};
                }
            }

            if (begin->type == token_type::bracket_round_open && (end - 1)->type == token_type::bracket_round_close) {
                return parse_where(resource, begin + 1, end - 1, statement);
            }

            if (mask_not == *begin) {
                auto child = parse_where(resource, begin + 1, end, statement);
                if (child.error.is_error()) {
                    return child;
                }
                auto expr = expressions::make_compare_union_expression(resource, expressions::compare_type::union_not);
                expr->append_child(child.expr);
                return {parser_result{true}, expr};
            }

            if (begin + 3 != end) {
                return {parser_result{parse_error::syntax_error, *(begin + 3), "not valid where condition"}, nullptr};
            }

            if (is_token_field_name(*begin) && is_token_operator(*(begin + 1)) && is_token_field_value(*(begin + 2))) {
                return {
                    parser_result{true},
                    expressions::make_compare_expression(
                        resource,
                        get_compare_expression(*(begin + 1)),
                        expressions::key_t{token_clean_value(*begin)},
                        statement.add_parameter(parse_value(*(begin + 2), statement.parameters().tape(), resource)))};
            } else if (is_token_field_value(*begin) && is_token_operator(*(begin + 1)) &&
                       is_token_field_name(*(begin + 2))) {
                return {parser_result{true},
                        expressions::make_compare_expression(
                            resource,
                            get_compare_expression(*(begin + 1)),
                            expressions::key_t{token_clean_value(*(begin + 2))},
                            statement.add_parameter(parse_value(*begin, statement.parameters().tape(), resource)))};
            }

            return {parser_result{false}, nullptr};
        }

        parser_result parse_join_on_expression(std::pmr::memory_resource* resource,
                                               lexer_t& lexer,
                                               expressions::join_expression_field& expr) {
            auto token = lexer.current_significant_token();
            if (token.type != token_type::bare_word) {
                return parser_result{parse_error::not_valid_join_condition, token, "not valid join condition"};
            }
            expr.collection.collection = token.value();
            token = lexer.next_token();
            if (token.type != token_type::dot) {
                return parser_result{parse_error::not_valid_join_condition, token, "not valid join condition"};
            }
            token = lexer.next_token();
            if (token.type == token_type::bare_word && lexer.next_token().type == token_type::dot) {
                expr.collection.database = expr.collection.collection;
                expr.collection.collection = token.value();
                token = lexer.next_token();
            }
            if (!is_token_field_name(token)) {
                return parser_result{parse_error::not_valid_join_condition, token, "not valid join condition"};
            }
            expr.expr = expressions::make_scalar_expression(resource,
                                                            expressions::scalar_type::get_field,
                                                            expressions::key_t{token_clean_value(token)});
            lexer.next_not_whitespace_token();
            return true;
        }

    } // namespace

    parser_result parse_where(std::pmr::memory_resource* resource,
                              lexer_t& lexer,
                              ql::aggregate::match_t& match,
                              ql::ql_param_statement_t& statement) {
        std::pmr::vector<token_t> tokens(resource);
        auto token = lexer.next_not_whitespace_token();
        while (!is_token_end_query(token) && !is_token_where_end(token)) {
            tokens.push_back(token);
            token = lexer.next_not_whitespace_token();
        }
        auto result = parse_where(resource, tokens.begin(), tokens.end(), statement);
        if (result.error.is_error()) {
            return result.error;
        }
        match.query = result.expr;
        if (!match.query) {
            return parser_result{parse_error::not_valid_where_condition, token, "not valid where condition"};
        }
        return true;
    }

    parser_result parse_join_on(std::pmr::memory_resource* resource, lexer_t& lexer, ql::join_t& join) {
        auto token = lexer.current_significant_token();
        while (!is_token_end_query(token) && !is_token_join_end(token)) {
            expressions::join_expression_field left;
            expressions::join_expression_field right;
            auto res = parse_join_on_expression(resource, lexer, left);
            if (res.is_error()) {
                return res;
            }
            token = lexer.current_significant_token();
            auto compare = get_compare_expression(token);
            if (compare == expressions::compare_type::invalid) {
                return parser_result{parse_error::not_valid_join_condition, token, "not valid join condition"};
            }
            token = lexer.next_not_whitespace_token();
            res = parse_join_on_expression(resource, lexer, right);
            if (res.is_error()) {
                return res;
            }
            join.expressions.push_back(
                expressions::make_join_expression(resource, compare, std::move(left), std::move(right)));
            token = lexer.current_significant_token();
            if (!is_token_end_query(token) && !is_token_join_end(token) && !is_token_join_on_end(token)) {
                return parser_result{parse_error::not_valid_join_condition, token, "not valid join condition"};
            }
            if (is_token_join_on_end(token)) {
                token = lexer.next_not_whitespace_token();
            }
        }
        return true;
    }

} // namespace components::sql::impl
