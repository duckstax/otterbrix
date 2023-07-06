#include "parser_insert_into.hpp"
#include <components/sql/parser/base/parser_mask.h>

using namespace components::sql::impl;

namespace components::sql::insert::impl {

    components::sql::impl::parser_result parse_insert_into(std::string_view query,
                                                           ql::variant_statement_t& statement,
                                                           std::pmr::memory_resource* resource) {
        static mask_t mask_begin({
            mask_element_t(token_type::bare_word, "insert"),
            mask_element_t(token_type::whitespace, ""),
            mask_element_t(token_type::bare_word, "into")
        });

        static const mask_element_t mask_elem_values(token_type::bare_word, "values");


        lexer_t lexer(query);
        if (!mask_begin.match(lexer)) {
            return false;
        }

        lexer.save();
        if (!contents_mask_element(lexer, mask_elem_values)) {
            return false;
        }
        lexer.restore();

        auto token = lexer.next_not_whitespace_token();
        if (token.type != token_type::bare_word) {
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid insert query"};
        }

        auto schema = std::string();
        auto table = std::string(token.value());

        token = lexer.next_token();
        if (token.type == token_type::dot) {
            token = lexer.next_token();
            if (token.type != token_type::bare_word) {
                return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid insert query"};
            }
            schema = table;
            table = std::string(token.value());
        }

        std::pmr::vector<std::string> fields(resource);
        auto res = parse_field_names(lexer, fields);
        if (res.is_error()) {
            return res;
        }

        token = lexer.next_not_whitespace_token();
        if (mask_elem_values != token) {
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid insert query"};
        }

        std::pmr::vector<components::document::document_ptr> documents(resource);

        bool is_first = true;
        while (is_first || (token = lexer.next_not_whitespace_token()).type == token_type::comma) {
            std::pmr::vector<::document::wrapper_value_t> values(resource);
            res = parse_field_values(lexer, values);
            if (res.is_error()) {
                return res;
            }
            if (values.size() != fields.size()) {
                return components::sql::impl::parser_result{parse_error::not_valid_size_values_list, lexer.next_token(), "not valid insert query"};
            }

            auto doc = document::make_document();
            auto it_field = fields.begin();
            for (auto it_value = values.begin(); it_value < values.end(); ++it_field, ++it_value) {
                doc->set(*it_field, **it_value);
            }
            documents.push_back(doc);
            is_first = false;
        }

        if (!is_token_end_query(token)) {
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid insert query"};
        }

        statement = ql::insert_many_t{schema, table, documents};
        return true;
    }

} // namespace components::sql::insert::impl
