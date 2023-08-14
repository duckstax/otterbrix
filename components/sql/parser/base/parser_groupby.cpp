#include "parser_groupby.hpp"
#include <array>
#include <memory_resource>
#include <components/sql/parser/base/parser_mask.hpp>

namespace components::sql::impl {

    namespace {

        static const std::array<mask_element_t, 1> groupby_stop_words {
            mask_element_t(token_type::bare_word, "having")
        };

        inline bool is_token_groupby_end(const token_t& token) {
            return std::find_if(groupby_stop_words.begin(), groupby_stop_words.end(),
                                [&](const mask_element_t& elem){
                return elem == token;
            }) != groupby_stop_words.end();
        }

    } // namespace

    parser_result parse_groupby(lexer_t& lexer, std::pmr::set<std::string_view>& group_fields) {
        auto token = lexer.next_not_whitespace_token();
        while (!is_token_end_query(token) && !is_token_groupby_end(token)) {
            if (!is_token_field_name(token)) {
                return parser_result{parse_error::syntax_error, token, "not valid group by condition"};
            }
            group_fields.insert(token.value());
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

    parser_result check_groupby(const ql::aggregate::group_t& group, const std::pmr::set<std::string_view>& group_fields) {
        auto it = std::find_if(group.fields.begin(), group.fields.end(), [](const expressions::expression_ptr& expr) {
            return expr->group() == expressions::expression_group::aggregate;
        });
        if (it == group.fields.end() && group_fields.empty()) {
            return true;
        }

        std::pmr::set<std::string_view> buf(group_fields);
        for (const auto& field : group.fields) {
            if (field->group() == expressions::expression_group::scalar) {
                auto name = field->to_string();
                if (group_fields.find(std::string_view(name)) == group_fields.end()) {
                    return parser_result{parse_error::group_by_less_paramaters, token_t(), "less group by fields"};
                    //todo: token error
                }
                buf.erase(std::string_view(name));
            }
        }
        if (!buf.empty()) {
            return parser_result{parse_error::group_by_more_paramaters, token_t(), "more group by fields"};
            //todo: token error
        }
        return true;
    }

} // namespace components::sql::impl
