#include "parser_select_base.hpp"
#include <components/sql/parser/base/parser_mask.hpp>
#include <components/sql/parser/base/parser_orderby.hpp>
#include <components/sql/parser/base/parser_where.hpp>

using namespace components::sql::impl;

namespace components::sql::select::impl {

    components::sql::impl::parser_result parse_select_base(std::pmr::memory_resource* resource,
                                                           std::string_view query,
                                                           ql::variant_statement_t& statement) {

        static const mask_element_t mask_elem_select(token_type::bare_word, "select");
        static const mask_element_t mask_elem_from(token_type::bare_word, "from");
        static const mask_element_t mask_elem_where(token_type::bare_word, "where");
        static const mask_group_element_t mask_elem_order({"order", "by"});

        lexer_t lexer(query);

        auto token = lexer.next_not_whitespace_token();
        if (mask_elem_select != token) {
            return false;
        }

        lexer.save();
        if (!contents_mask_element(lexer, mask_elem_from)) {
            return false;
        }
        lexer.restore();

        token = lexer.next_not_whitespace_token();
        while (mask_elem_from != token) {
            //todo: parse field
            token = lexer.next_not_whitespace_token();
        }

        auto schema = std::string();
        auto table = std::string();
        token = lexer.next_not_whitespace_token();
        if (token.type == token_type::bare_word) {
            table = std::string(token.value());
            token = lexer.next_token();
            if (token.type == token_type::dot) {
                token = lexer.next_token();
                if (token.type != token_type::bare_word) {
                    return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid select query"};
                }
                schema = table;
                table = std::string(token.value());
            }
            statement = ql::aggregate_statement{schema, table};
            auto& agg = std::get<ql::aggregate_statement>(statement);

            // where
            token = lexer.next_not_whitespace_token();
            if (mask_elem_where == token) {
                auto match = ql::aggregate::make_match(nullptr);
                auto res = parse_where(resource, lexer, match, agg);
                if (res.is_error()) {
                    return res;
                }
                agg.append(ql::aggregate::operator_type::match, match);
            }

            // order by
            auto status_order = mask_elem_order.check(lexer);
            if (status_order == mask_group_element_t::status::error) {
                return components::sql::impl::parser_result{parse_error::syntax_error, lexer.next_not_whitespace_token(), "invalid use order"};
            }
            if (status_order == mask_group_element_t::status::yes) {
                ql::aggregate::sort_t sort;
                auto res = parse_orderby(resource, lexer, sort);
                if (res.is_error()) {
                    return res;
                }
                agg.append(ql::aggregate::operator_type::sort, sort);
            }

            token = lexer.current_significant_token();
            if (!is_token_end_query(token)) {
                statement = ql::unused_statement_t{};
                return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid update query"};
            }

        } else {
            //todo: other variants
            return false;
        }

        return true;
    }

} // namespace components::sql::select::impl
