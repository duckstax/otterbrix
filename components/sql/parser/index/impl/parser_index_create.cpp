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
    // CREATE INDEX MY_INDEX ON TEST_DATABASE.TEST_COLLECTION USING HASH (count)

    components::sql::impl::parser_result
    parse_create(std::pmr::memory_resource*, std::string_view query, ql::variant_statement_t& statement) {
        static mask_t index_base_mask({mask_element_t(token_type::bare_word, "create"),
                                       mask_element_t(token_type::whitespace, ""),
                                       mask_element_t(token_type::bare_word, "index")});

        static const mask_element_t mask_elem_on(token_type::bare_word, "on");
        static const mask_element_t mask_elem_using(token_type::bare_word, "using");

        // pase CREATE INDEX
        lexer_t lexer(query);
        if (!index_base_mask.match(lexer)) {
            return false;
        }

        // check ON in query
        lexer.save();
        if (!contains_mask_element(lexer, mask_elem_on)) {
            return false;
            // TODO add error result for miss indexes
            // return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid index query"};
        }
        lexer.restore();

        // check USING in query
        lexer.save();
        if (contains_mask_element(lexer, mask_elem_using)) {
            return false; // Should contain 'unsupported' in error
            // TODO add error result for miss indexes
            // return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid index query"};
        }
        lexer.restore();

        // parse MY_INDEX ON TEST_DATABASE.TEST_COLLECTION
        // Get index name token
        // TODO move to lambda or new method
        std::string index_name;
        {
            auto index_name_token = lexer.next_not_whitespace_token();
            if (index_name_token.type != token_type::bare_word || mask_elem_on == index_name_token) {
                return components::sql::impl::parser_result{parse_error::syntax_error,
                                                            index_name_token,
                                                            "not valid index query"};
            }
            index_name = std::string(index_name_token.value());
        }

        // parse ON
        if (const auto token = lexer.next_not_whitespace_token(); mask_elem_on != token) {
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid index query"};
        }

        // Get database name token
        std::string database_name;
        {
            auto database_name_token = lexer.next_not_whitespace_token();
            if (database_name_token.type != token_type::bare_word) {
                return components::sql::impl::parser_result{parse_error::syntax_error,
                                                            database_name_token,
                                                            "not valid index query"};
            }
            database_name = std::string(database_name_token.value());
        }

        if (const auto token = lexer.next_token(); token.type != token_type::dot) {
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid index query"};
        }

        std::string collection_name;
        {
            // Get collection name token
            auto collection_name_token = lexer.next_not_whitespace_token();
            if (collection_name_token.type != token_type::bare_word) {
                return components::sql::impl::parser_result{parse_error::syntax_error,
                                                            collection_name_token,
                                                            "not valid index query"};
            }
            collection_name = std::string(collection_name_token.value());
        }

        if (const auto token = lexer.next_not_whitespace_token(); token.type != token_type::bracket_round_open) {
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid index query"};
        }

        // parse (COLUMN1, COLUMN2, ... ) or (COLUMN)
        bool unsupported = false;
        std::vector<std::string> key_names;
        auto token = lexer.next_not_whitespace_token();
        while (token.type != token_type::bracket_round_close) {
            if (token.type == token_type::bare_word) {
                key_names.push_back(std::string(token.value()));
                token = lexer.next_not_whitespace_token();
                continue;
            } else if (token.type == token_type::comma) {
                unsupported = true;
                break;
            }
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid index query"};
        }

        if (const auto token = lexer.next_token(); !is_token_end_query(token)) {
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid index query"};
        }

        // fill create index or send error

        if (unsupported) {
            return components::sql::impl::parser_result{parse_error::syntax_error,
                                                        token,
                                                        "multi key index is not supported"};
        }

        // For now we support only single key
        assert(key_names.size() == 1);
        auto crate_index = components::ql::create_index_t(database_name, collection_name, index_name);
        for (auto&& key : key_names) {
            crate_index.keys_.emplace_back(components::expressions::key_t{std::move(key)});
        }
        statement = std::move(crate_index);

        return true;
    }

    // TODO without db

} // namespace components::sql::index::impl
