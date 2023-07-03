#include "parser_insert_into.hpp"
#include <components/sql/parser/base/parser_mask.h>

using namespace components::sql::impl;

namespace components::sql::insert::impl {

    parser_result parse_insert_into(std::string_view query, ql::variant_statement_t& statement) {
        static mask_t mask_begin({
            mask_element_t(token_type::bare_word, "insert"),
            mask_element_t(token_type::whitespace, ""),
            mask_element_t(token_type::bare_word, "into"),
            mask_element_t(token_type::whitespace, ""),
        });

        static const mask_element_t mask_elem_values(token_type::bare_word, "values");


        lexer_t lexer(query);
        if (!mask_begin.match(lexer)) {
            return false;
        }

        lexer.save();
        if (contents_mask_element(lexer, mask_elem_values)) {
            return false;
        }
        lexer.restore();

        auto token = lexer.next_token();
        if (token.type != token_type::bare_word) {
            //todo: error
            return false;
        }
    }

} // namespace components::sql::insert::impl
