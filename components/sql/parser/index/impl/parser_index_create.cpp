#include "parser_index_create.hpp"
#include <components/sql/parser/base/parser_mask.hpp>

using components::sql::impl::mask_element_t;
using components::sql::impl::mask_t;

namespace components::sql::index::impl {

    constexpr uint64_t index_name = 4;
    constexpr uint64_t database_name = 7;
    constexpr uint64_t collection_name = 9;
    constexpr uint64_t column_name = 11;

    // CREATE INDEX MY_INDEX ON TEST_DATABASE.TEST_COLLECTION (count)
    // TODO add user index name
    // TODO index type? (single...)
    // TODO compare type (int64...)

    components::sql::impl::parser_result
    parse_create(std::pmr::memory_resource*, std::string_view query, ql::variant_statement_t& statement) {
        static mask_t mask({mask_element_t(token_type::bare_word, "create"),
                            mask_element_t(token_type::whitespace, ""),
                            mask_element_t(token_type::bare_word, "index"),
                            mask_element_t(token_type::whitespace, ""),
                            mask_element_t::create_value_mask_element(), // index name 4
                            mask_element_t(token_type::whitespace, ""),
                            mask_element_t(token_type::bare_word, "on"),
                            mask_element_t::create_value_mask_element(), // database 7
                            mask_element_t(token_type::dot, "."),
                            mask_element_t::create_value_mask_element(), // collection 9
                            mask_element_t(token_type::bracket_round_open, "("),
                            mask_element_t::create_value_mask_element(), // column 11
                            mask_element_t(token_type::bracket_round_close, ")"),
                            mask_element_t(token_type::semicolon, ";", true)});

        lexer_t lexer(query);
        if (mask.match(lexer)) {
            statement = components::ql::create_index_t{
                mask.cap(index_name),
                mask_with_db.cap(database_name),
                mask_with_db.cap(collection_name),
                mask_with_db.cap(column_name),
            };
            return true;
        }
        return false;
    }

    // TODO without db

} // namespace components::sql::index::impl
