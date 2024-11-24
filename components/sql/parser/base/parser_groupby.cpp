#include "parser_groupby.hpp"
#include <algorithm>
#include <array>
#include <components/sql/parser/base/parser_mask.hpp>
#include <memory_resource>

namespace components::sql::impl {

    namespace {

        static const std::array<mask_element_t, 2> groupby_stop_words{mask_element_t(token_type::bare_word, "having"),
                                                                      mask_element_t(token_type::bare_word, "order")};

        inline bool is_token_groupby_end(const token_t& token) {
            return std::find_if(groupby_stop_words.begin(), groupby_stop_words.end(), [&](const mask_element_t& elem) {
                       return elem == token;
                   }) != groupby_stop_words.end();
        }

    } // namespace

    parser_result parse_groupby(lexer_t& lexer, std::pmr::set<token_t>& group_fields) {
        auto token = lexer.next_not_whitespace_token();
        while (!is_token_end_query(token) && !is_token_groupby_end(token)) {
            if (!is_token_field_name(token)) {
                return parser_result{parse_error::syntax_error, token, "not valid group by condition"};
            }
            group_fields.insert(token);
            token = lexer.next_not_whitespace_token();
            if (token.type == token_type::comma) {
                token = lexer.next_not_whitespace_token();
                if (is_token_groupby_end(token) || is_token_end_query(token)) {
                    return parser_result{parse_error::syntax_error, token, "not valid group by condition"};
                }
            } else if (!is_token_groupby_end(token) && !is_token_end_query(token)) {
                return parser_result{parse_error::syntax_error, token, "not valid group by condition"};
            }
        }
        if (group_fields.empty()) {
            return parser_result{parse_error::empty_group_by_list, token, "empty group by fields"};
        }
        return true;
    }

    parser_result check_groupby(std::pmr::memory_resource* resource,
                                const ql::aggregate::group_t& group,
                                const std::pmr::set<token_t>& group_fields_select,
                                const std::pmr::set<token_t>& group_fields) {
        auto it = std::find_if(group.fields.begin(), group.fields.end(), [](const expressions::expression_ptr& expr) {
            return expr->group() == expressions::expression_group::aggregate;
        });
        if (it == group.fields.end() && group_fields.empty()) {
            return true;
        }

        std::pmr::set<token_t> diff(resource);
        std::set_difference(group_fields_select.begin(),
                            group_fields_select.end(),
                            group_fields.begin(),
                            group_fields.end(),
                            std::inserter(diff, diff.end()));
        if (!diff.empty()) {
            return parser_result{parse_error::group_by_less_paramaters, *diff.begin(), "less group by fields"};
        }

        std::set_difference(group_fields.begin(),
                            group_fields.end(),
                            group_fields_select.begin(),
                            group_fields_select.end(),
                            std::inserter(diff, diff.end()));
        if (!diff.empty()) {
            return parser_result{parse_error::group_by_more_paramaters, *diff.begin(), "more group by fields"};
        }

        return true;
    }

} // namespace components::sql::impl
