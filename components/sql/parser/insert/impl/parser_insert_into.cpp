#include "parser_insert_into.hpp"
#include <components/sql/parser/base/parser_mask.h>

using components::sql::impl::mask_t;
using components::sql::impl::mask_element_t;

namespace components::sql::insert::impl {

    constexpr uint64_t index_schema = 4;
    constexpr uint64_t index_table = 6;

    bool parseInsertInto(std::string_view query, ql::variant_statement_t& statement) {
        static mask_t mask({
            mask_element_t(token_type::bare_word, "insert"),
            mask_element_t(token_type::whitespace, ""),
            mask_element_t(token_type::bare_word, "into"),
            mask_element_t(token_type::whitespace, ""),
            mask_element_t::create_optional_value_mask_element(),
            mask_element_t(token_type::dot, ".", true),
            mask_element_t::create_value_mask_element(),
            mask_element_t(token_type::semicolon, ";", true)
        });

        lexer_t lexer(query);
        if (mask.match(lexer)) {
            std::pmr::vector<document::document_ptr> documents;
            statement = components::ql::insert_many_t{mask.cap(index_schema), mask.cap(index_table), documents};
            return true;
        }
        return false;
    }

} // namespace components::sql::insert::impl
