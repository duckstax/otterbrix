#include "parser_select_fields.hpp"
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/sql/parser/base/parser_mask.hpp>

using namespace components::sql::impl;

namespace components::sql::select::impl {

    namespace {

        static const mask_element_t fields_stop_word{token_type::bare_word, "from"};
        static const mask_element_t mask_as{token_type::bare_word, "as"};

    } // namespace

    parser_result parse_select_fields(std::pmr::memory_resource* resource,
                                      lexer_t& lexer,
                                      ql::aggregate::group_t& group,
                                      ql::ql_param_statement_t& statement) {
        auto token = lexer.current_significant_token();
        while (fields_stop_word != token && !is_token_end_query(token)) {
            if (token.type == token_type::asterisk) {
                //todo: add all fields from structure
            } else if (is_token_field_name(token)) {
                expressions::key_t key_value{token_clean_value(token)};
                token = lexer.next_not_whitespace_token();
                if (mask_as == token) {
                    token = lexer.next_not_whitespace_token();
                    if (!is_token_field_name(token)) {
                        return parser_result{parse_error::syntax_error, token, "not valid select query"};
                    }
                }
                if (is_token_field_name(token) && fields_stop_word != token) {
                    expressions::key_t key_name{token_clean_value(token)};
                    append_expr(group, expressions::make_scalar_expression(resource, expressions::scalar_type::get_field, key_name, key_value));
                    token = lexer.next_not_whitespace_token();
                } else {
                    append_expr(group, expressions::make_scalar_expression(resource, expressions::scalar_type::get_field, key_value));
                }
                if (token.type != token_type::comma && fields_stop_word != token) {
                    return parser_result{parse_error::syntax_error, token, "not valid select query"};
                }
            } else if (is_token_field_value(token)) {
                auto value = parse_value(token);
                expressions::key_t key_name{token_clean_value(token)};
                token = lexer.next_not_whitespace_token();
                if (mask_as == token) {
                    token = lexer.next_not_whitespace_token();
                }
                if (is_token_field_name(token)) {
                    key_name = expressions::key_t{token_clean_value(token)};
                    token = lexer.next_not_whitespace_token();
                }
                auto expr = expressions::make_scalar_expression(resource, expressions::scalar_type::get_field, key_name);
                expr->append_param(statement.add_parameter(value));
                append_expr(group, expr);
                if (token.type != token_type::comma && fields_stop_word != token && !is_token_end_query(token)) {
                    return parser_result{parse_error::syntax_error, token, "not valid select query"};
                }
            } else {
                return parser_result{parse_error::syntax_error, token, "not valid select query"};
            }
            if (fields_stop_word != token && !is_token_end_query(token)) {
                token = lexer.next_not_whitespace_token();
            }
        }

        return true;
    }

} // namespace components::sql::select::impl
