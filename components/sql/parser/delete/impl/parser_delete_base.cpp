#include "parser_delete_base.hpp"
#include <components/sql/parser/base/parser_mask.hpp>
#include <components/sql/parser/base/parser_where.hpp>

using namespace components::sql::impl;

namespace components::sql::delete_::impl {

    components::sql::impl::parser_result
    parse_delete_base(std::pmr::memory_resource* resource, std::string_view query, ql::variant_statement_t& statement) {
        static const mask_element_t mask_elem_delete(token_type::bare_word, "delete");
        static const mask_element_t mask_elem_from(token_type::bare_word, "from");
        static const mask_element_t mask_elem_where(token_type::bare_word, "where");

        lexer_t lexer(query);

        auto token = lexer.next_not_whitespace_token();
        if (mask_elem_delete != token) {
            return false;
        }

        token = lexer.next_not_whitespace_token();
        if (mask_elem_from != token) {
            return false;
        }

        token = lexer.next_not_whitespace_token();
        if (token.type != token_type::bare_word) {
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid delete query"};
        }

        auto schema = std::string();
        auto table = std::string(token.value());
        token = lexer.next_token();
        if (token.type == token_type::dot) {
            token = lexer.next_token();
            if (token.type != token_type::bare_word) {
                return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid delete query"};
            }
            schema = table;
            table = std::string(token.value());
        }
        statement = ql::delete_many_t{schema, table, resource};
        assert(std::holds_alternative<ql::delete_many_t>(statement) &&
               "[components::sql::impl::parser_result parse_delete_base]: [ql::delete_many_t] variant statement holds "
               "the alternative");
        auto& del = std::get<ql::delete_many_t>(statement);

        token = lexer.next_not_whitespace_token();
        if (mask_elem_where == token) {
            auto res = parse_where(resource, lexer, del.match_, del);
            if (res.is_error()) {
                statement = ql::unused_statement_t{};
                return res;
            }
            token = lexer.current_significant_token();
            if (!is_token_end_query(token)) {
                statement = ql::unused_statement_t{};
                return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid delete query"};
            }
        }

        return true;
    }

} // namespace components::sql::delete_::impl
