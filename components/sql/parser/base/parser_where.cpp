#include "parser_where.hpp"
#include <components/sql/parser/base/parser_mask.hpp>
#include <components/expressions/expression.hpp>

namespace components::sql::impl {

    namespace {

        static const std::vector<mask_element_t> where_stop_words {
            mask_element_t{token_type::bare_word, "group"},
            mask_element_t{token_type::bare_word, "order"},
            mask_element_t{token_type::bare_word, "limit"}
        };

        static const mask_element_t mask_not{token_type::bare_word, "not"};
        static const mask_element_t mask_and{token_type::bare_word, "and"};
        static const mask_element_t mask_or{token_type::bare_word, "or"};

        inline bool is_token_where_end(const token_t& token) {
            return std::find_if(where_stop_words.begin(), where_stop_words.end(),
                                [&](const mask_element_t& elem){
                return elem == token;
            }) != where_stop_words.end();
        }

        inline bool is_token_operand(const token_t& token) {
            return is_token_field_name(token)
                || is_token_field_value(token);
        }

        inline bool is_token_operator(const token_t& token) {
            return token.type == token_type::equals
                || token.type == token_type::not_equals
                || token.type == token_type::less
                || token.type == token_type::less_or_equals
                || token.type == token_type::greater
                || token.type == token_type::greater_or_equals;
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
                default: return expressions::compare_type::invalid;
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
                        return {parser_result{parse_error::not_exists_open_round_bracket, *it, "not valid where condition"}, nullptr};
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
                    auto expr = expressions::make_compare_union_expression(resource,
                                                                           mask_and == *it
                                                                           ? expressions::compare_type::union_and
                                                                           : expressions::compare_type::union_or);
                    auto append = [&expr](expressions::compare_expression_ptr& e) {
                        if (expr->type() == e->type()) {
                            for (auto &child : e->children()) {
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

            if (begin->type == token_type::bracket_round_open
                && (end - 1)->type == token_type::bracket_round_close) {
                return parse_where(resource, begin + 1, end - 1, statement);
            }

            if (is_token_field_name(*begin)
                && is_token_operator(*(begin + 1))
                && is_token_field_value(*(begin + 2))) {
                return {parser_result{true},
                    expressions::make_compare_expression(resource,
                                                         get_compare_expression(*(begin + 1)),
                                                         expressions::key_t{token_clean_value(*begin)},
                                                         statement.add_parameter(parse_value(*(begin + 2))))};
            } else if (is_token_field_value(*begin)
                       && is_token_operator(*(begin + 1))
                       && is_token_field_name(*(begin + 2))) {
                return {parser_result{true},
                    expressions::make_compare_expression(resource,
                                                         get_compare_expression(*(begin + 1)),
                                                         expressions::key_t{token_clean_value(*(begin + 2))},
                                                         statement.add_parameter(parse_value(*begin)))};
            }

            return {parser_result{false}, nullptr};
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

} // namespace components::sql::impl
