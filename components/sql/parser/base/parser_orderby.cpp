#include "parser_orderby.hpp"
#include <algorithm>
#include <components/expressions/expression.hpp>
#include <components/sql/parser/base/parser_mask.hpp>

namespace components::sql::impl {

    namespace {

        static const mask_element_t mask_asc{token_type::bare_word, "asc"};
        static const mask_element_t mask_desc{token_type::bare_word, "desc"};

        static const std::vector<mask_element_t> orderby_group_stop_words{mask_asc, mask_desc};

        static const std::vector<mask_element_t> orderby_stop_words{mask_element_t{token_type::bare_word, "limit"}};

        inline bool is_token_orderby_end(const token_t& token) {
            return std::find_if(orderby_stop_words.begin(), orderby_stop_words.end(), [&](const mask_element_t& elem) {
                       return elem == token;
                   }) != orderby_stop_words.end();
        }

        inline bool is_token_orderby_group_end(const token_t& token) {
            return std::find_if(orderby_group_stop_words.begin(),
                                orderby_group_stop_words.end(),
                                [&](const mask_element_t& elem) { return elem == token; }) !=
                   orderby_group_stop_words.end();
        }

    } // namespace

    parser_result
    parse_orderby(std::pmr::memory_resource* resource, lexer_t& lexer, components::ql::aggregate::sort_t& sort) {
        auto token = lexer.next_not_whitespace_token();
        while (!is_token_end_query(token) && !is_token_orderby_end(token)) {
            std::pmr::vector<std::string_view> fields(resource);
            while (!is_token_end_query(token) && !is_token_orderby_end(token) && !is_token_orderby_group_end(token)) {
                if (is_token_field_name(token)) {
                    fields.push_back(token_clean_value(token));
                }
                token = lexer.next_not_whitespace_token();
                if (token.type == token_type::comma) {
                    token = lexer.next_not_whitespace_token();
                } else if (!is_token_end_query(token) && !is_token_orderby_end(token) &&
                           !is_token_orderby_group_end(token)) {
                    return parser_result{parse_error::syntax_error, token, "not valid order by condition"};
                }
            }
            auto order = components::expressions::sort_order::asc;
            if (mask_desc == token) {
                order = components::expressions::sort_order::desc;
            }
            std::for_each(fields.begin(), fields.end(), [&sort, order](std::string_view key) {
                components::ql::aggregate::append_sort(sort, components::expressions::key_t{key}, order);
            });
            token = lexer.next_not_whitespace_token();
            if (token.type == token_type::comma) {
                token = lexer.next_not_whitespace_token();
                if (is_token_end_query(token) || is_token_orderby_end(token)) {
                    return parser_result{parse_error::syntax_error, token, "not valid order by condition"};
                }
            }
        }
        if (sort.values.empty()) {
            return parser_result{parse_error::empty_order_by_list, token, "empty order by fields"};
        }
        return true;
    }

} // namespace components::sql::impl
