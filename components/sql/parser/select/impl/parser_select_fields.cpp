#include "parser_select_fields.hpp"
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/sql/parser/base/parser_mask.hpp>

using namespace components::sql::impl;

namespace components::sql::select::impl {

    namespace {

        static const mask_element_t fields_stop_word{token_type::bare_word, "from"};
        static const mask_element_t mask_as{token_type::bare_word, "as"};

        static const mask_element_t mask_group_sum{token_type::bare_word, "sum"};
        static const mask_element_t mask_group_count{token_type::bare_word, "count"};
        static const mask_element_t mask_group_min{token_type::bare_word, "min"};
        static const mask_element_t mask_group_max{token_type::bare_word, "max"};
        static const mask_element_t mask_group_avg{token_type::bare_word, "avg"};

        static const mask_element_t group_words[] {
            mask_group_sum,
            mask_group_count,
            mask_group_min,
            mask_group_max,
            mask_group_avg
        };

        inline bool is_token_group_word(const token_t& token) {
            constexpr auto size = sizeof(group_words) / sizeof(mask_element_t);
            return std::find_if(group_words, group_words + size,
                                [&](const mask_element_t& elem){
                return elem == token;
            }) != group_words + size;
        }

        inline expressions::aggregate_type get_aggregate_expression(const token_t& token) {
            if (mask_group_sum == token)    return expressions::aggregate_type::sum;
            if (mask_group_count == token)  return expressions::aggregate_type::count;
            if (mask_group_min == token)    return expressions::aggregate_type::min;
            if (mask_group_max == token)    return expressions::aggregate_type::max;
            if (mask_group_avg == token)    return expressions::aggregate_type::avg;
            return expressions::aggregate_type::invalid;
        }

        parser_result parse_select_field(std::pmr::memory_resource* resource,
                                         lexer_t& lexer,
                                         ql::aggregate::group_t& group) {
            auto token = lexer.current_significant_token();
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

            return true;
        }

        parser_result parse_select_constant(std::pmr::memory_resource* resource,
                                            lexer_t& lexer,
                                            ql::aggregate::group_t& group,
                                            ql::ql_param_statement_t& statement) {
            auto token = lexer.current_significant_token();
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
            return true;
        }

    } // namespace

    parser_result parse_select_fields(std::pmr::memory_resource* resource,
                                      lexer_t& lexer,
                                      ql::aggregate::group_t& group,
                                      ql::ql_param_statement_t& statement) {
        auto token = lexer.current_significant_token();
        while (fields_stop_word != token && !is_token_end_query(token)) {
            if (token.type == token_type::asterisk) {
                //todo: add all fields from structure
                token = lexer.next_not_whitespace_token();
            } else if (is_token_group_word(token)) {
                //todo: group
            } else if (is_token_field_name(token)) {
                auto res = parse_select_field(resource, lexer, group);
                if (res.is_error()) {
                    return res;
                }
            } else if (is_token_field_value(token)) {
                auto res = parse_select_constant(resource, lexer, group, statement);
                if (res.is_error()) {
                    return res;
                }
            } else {
                return parser_result{parse_error::syntax_error, token, "not valid select query"};
            }
            token = lexer.current_significant_token();
            if (fields_stop_word != token && !is_token_end_query(token)) {
                token = lexer.next_not_whitespace_token();
            }
        }

        return true;
    }

} // namespace components::sql::select::impl
