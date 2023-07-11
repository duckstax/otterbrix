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

    } // namespace


    parser_result parse_where(std::pmr::memory_resource* resource,
                              lexer_t& lexer,
                              ql::aggregate::match_t& match,
                              ql::ql_param_statement_t& statement) {
        auto token = lexer.next_not_whitespace_token();
        while (!is_token_end_query(token) && !is_token_where_end(token)) {
            if (!is_token_operand(token)) {
                return parser_result{parse_error::syntax_error, token, "not valid where condition"};
            }
            auto op1 = token;
            token = lexer.next_not_whitespace_token();
            if (!is_token_operator(token)) {
                return parser_result{parse_error::syntax_error, token, "not valid where condition"};
            }
            auto op = token;
            token = lexer.next_not_whitespace_token();
            if (!is_token_operand(token)) {
                return parser_result{parse_error::syntax_error, token, "not valid where condition"};
            }
            auto op2 = token;
            if (is_token_field_name(op1) && is_token_field_value(op2)) {
                match.query = expressions::make_compare_expression(resource,
                                                                   get_compare_expression(op),
                                                                   expressions::key_t{token_clean_value(op1)},
                                                                   statement.add_parameter(parse_value(op2)));
            } else if (is_token_field_value(op1) && is_token_field_name(op2)) {
                match.query = expressions::make_compare_expression(resource,
                                                                   get_compare_expression(op),
                                                                   expressions::key_t{token_clean_value(op2)},
                                                                   statement.add_parameter(parse_value(op1)));
            } else {
                //todo: other variants
                return parser_result{parse_error::syntax_error, op2, "not valid where condition"};
            }
            token = lexer.next_not_whitespace_token();
        }
        if (!match.query) {
            return parser_result{parse_error::not_valid_where_condition, token, "not valid where condition"};
        }
        return true;
    }

} // namespace components::sql::impl
