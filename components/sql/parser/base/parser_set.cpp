#include "parser_set.hpp"
#include <components/sql/parser/base/parser_mask.hpp>

namespace components::sql::impl {

    parser_result parse_set(lexer_t& lexer, document::document_ptr& doc) {
        auto doc_value = document::make_document(doc->get_allocator());
        auto tape = std::make_unique<document::impl::base_document>(doc->get_allocator());
        auto token = lexer.current_significant_token();
        do {
            token = lexer.next_not_whitespace_token();
            if (!is_token_field_name(token)) {
                return parser_result{parse_error::syntax_error, token, "not valid update query"};
            }
            auto key = std::string{token.value()};
            token = lexer.next_not_whitespace_token();
            if (token.type != token_type::equals) {
                return parser_result{parse_error::syntax_error, token, "not valid update query"};
            }
            token = lexer.next_not_whitespace_token();
            if (!is_token_field_value(token)) {
                return parser_result{parse_error::syntax_error, token, "not valid update query"};
            }
            auto value = parse_value(token, tape.get(), doc->get_allocator());
            if (!value) {
                return parser_result{parse_error::not_valid_value, token, "not valid value in update query"};
            }
            doc_value->set(key, value);
            token = lexer.next_not_whitespace_token();
        } while (token.type == token_type::comma);

        doc->set("$set", doc_value);
        return true;
    }

} // namespace components::sql::impl
