#include "parser_database_create.hpp"
#include <components/sql/parser/base/parser_mask.h>

using components::sql::impl::mask_t;
using components::sql::impl::mask_element_t;

namespace components::sql::database::impl {

    constexpr uint64_t index_name = 4;

    parser_result parse_create(std::string_view query, ql::variant_statement_t& statement) {
        static mask_t mask({
            mask_element_t(token_type::bare_word, "create"),
            mask_element_t(token_type::whitespace, ""),
            mask_element_t(token_type::bare_word, "database"),
            mask_element_t(token_type::whitespace, ""),
            mask_element_t::create_value_mask_element(),
            mask_element_t(token_type::semicolon, ";", true)
        });

        lexer_t lexer(query);
        if (mask.match(lexer)) {
            statement = components::ql::create_database_t{mask.cap(index_name)};
            return true;
        }
        return false;
    }

} // namespace components::sql::database::impl
