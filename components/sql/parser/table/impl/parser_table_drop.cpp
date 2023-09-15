#include "parser_table_drop.hpp"
#include <components/sql/parser/base/parser_mask.hpp>

using components::sql::impl::mask_t;
using components::sql::impl::mask_element_t;

namespace components::sql::table::impl {

    constexpr uint64_t index_name_db = 4;
    constexpr uint64_t index_name_table = 6;
    constexpr uint64_t index_name_table_without_db = 4;

    components::sql::impl::parser_result parse_drop(std::pmr::memory_resource*,
                                                    std::string_view query,
                                                    ql::variant_statement_t& statement) {
        static mask_t mask_with_db({
            mask_element_t(token_type::bare_word, "drop"),
            mask_element_t(token_type::whitespace, ""),
            mask_element_t(token_type::bare_word, "table"),
            mask_element_t(token_type::whitespace, ""),
            mask_element_t::create_value_mask_element(),
            mask_element_t(token_type::dot, "."),
            mask_element_t::create_value_mask_element(),
            mask_element_t(token_type::semicolon, ";", true)
        });

        static mask_t mask_without_db({
            mask_element_t(token_type::bare_word, "drop"),
            mask_element_t(token_type::whitespace, ""),
            mask_element_t(token_type::bare_word, "table"),
            mask_element_t(token_type::whitespace, ""),
            mask_element_t::create_value_mask_element(),
            mask_element_t(token_type::semicolon, ";", true)
        });

        lexer_t lexer(query);
        lexer.save();
        if (mask_with_db.match(lexer)) {
            statement = components::ql::drop_collection_t{
                    mask_with_db.cap(index_name_db),
                    mask_with_db.cap(index_name_table)
            };
            return true;
        }
        lexer.restore();
        if (mask_without_db.match(lexer)) {
            statement = components::ql::drop_collection_t{
                    database_name_t(),
                    mask_without_db.cap(index_name_table_without_db)
            };
            return true;
        }
        return false;
    }

} // namespace components::sql::table::impl
