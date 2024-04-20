#include "parser_update_base.hpp"
#include <components/sql/parser/base/parser_mask.hpp>
#include <components/sql/parser/base/parser_set.hpp>
#include <components/sql/parser/base/parser_where.hpp>

using namespace components::sql::impl;

namespace components::sql::update::impl {

    components::sql::impl::parser_result
    parse_update_base(std::pmr::memory_resource* resource, std::string_view query, ql::variant_statement_t& statement) {
        static const mask_element_t mask_elem_update(token_type::bare_word, "update");
        static const mask_element_t mask_elem_set(token_type::bare_word, "set");
        static const mask_element_t mask_elem_where(token_type::bare_word, "where");

        lexer_t lexer(query);

        auto token = lexer.next_not_whitespace_token();
        if (mask_elem_update != token) {
            return false;
        }

        lexer.save();
        if (!contains_mask_element(lexer, mask_elem_set)) {
            return false;
        }
        lexer.restore();

        token = lexer.next_not_whitespace_token();
        if (token.type != token_type::bare_word) {
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid update query"};
        }

        auto schema = std::string();
        auto table = std::string(token.value());
        token = lexer.next_token();
        if (token.type == token_type::dot) {
            token = lexer.next_token();
            if (token.type != token_type::bare_word) {
                return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid update query"};
            }
            schema = table;
            table = std::string(token.value());
        }

        token = lexer.next_not_whitespace_token();
        if (mask_elem_set != token) {
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid update query"};
        }

        statement = ql::update_many_t{schema, table};
        assert(std::holds_alternative<ql::update_many_t>(statement) &&
               "[components::sql::impl::parser_result parse_update_base]: [ql::update_many_t] variant statement holds "
               "the alternative");
        auto& upd = std::get<ql::update_many_t>(statement);

        auto res = parse_set(resource, lexer, upd.update_);
        if (res.is_error()) {
            statement = ql::unused_statement_t{};
            return res;
        }

        token = lexer.current_significant_token();
        if (mask_elem_where == token) {
            auto res = parse_where(resource, lexer, upd.match_, upd);
            if (res.is_error()) {
                statement = ql::unused_statement_t{};
                return res;
            }
            token = lexer.current_significant_token();
            if (!is_token_end_query(token)) {
                statement = ql::unused_statement_t{};
                return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid update query"};
            }
        } else {
            upd.match_.query = expressions::make_compare_expression(resource, expressions::compare_type::all_true);
        }

        token = lexer.current_significant_token();
        if (!is_token_end_query(token)) {
            statement = ql::unused_statement_t{};
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid update query"};
        }

        return true;
    }

} // namespace components::sql::update::impl
