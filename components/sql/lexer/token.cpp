#include "token.hpp"
#include <magic_enum.hpp>
#include <components/sql/parser/base/parser_mask.hpp>

namespace components::sql {

    token_t::token_t(token_type type, const char* begin, const char* end)
        : type(type)
        , begin(begin)
        , end(end) {
    }

    token_t::token_t(token_type type)
        : token_t(type, nullptr, nullptr) {
    }

    std::size_t token_t::size() const {
        return end - begin;
    }

    std::string_view token_t::value() const {
        return std::string_view{begin, size()};
    }


    bool is_token_significant(const token_t& token) {
        return token.type != token_type::whitespace
            && token.type != token_type::comment;
    }

    bool is_token_error(const token_t& token) {
        return token.type >= token_type::error;
    }

    bool is_token_end(const token_t& token) {
        return token.type == token_type::end_query;
    }

    bool is_token_end_query(const token_t& token) {
        return token.type == token_type::end_query
            || token.type == token_type::semicolon;
    }

    bool is_token_bool_value_true(const token_t& token) {
        static const impl::mask_element_t mask{token_type::bare_word, "true"};
        return mask == token;
    }

    bool is_token_bool_value_false(const token_t& token) {
        static const impl::mask_element_t mask{token_type::bare_word, "false"};
        return mask == token;
    }

    bool is_token_bool_value(const token_t& token) {
        return is_token_bool_value_true(token) || is_token_bool_value_false(token);
    }

    bool is_token_field_name(const token_t& token) {
        return token.type == token_type::bare_word
            || token.type == token_type::quoted_identifier;
    }

    bool is_token_field_value(const token_t& token) {
        return token.type == token_type::number_literal
            || token.type == token_type::string_literal
            || is_token_bool_value(token);
    }

    std::string_view token_name(token_type type) {
        return magic_enum::enum_name(type);
    }

    std::string_view token_name(const token_t& token) {
        return token_name(token.type);
    }

    std::string_view token_clean_value(const token_t& token) {
        if (token.type == token_type::string_literal) {
            return std::string_view{token.value().data() + 1, token.value().size() - 2};
        }
        if (token.type == token_type::quoted_identifier) {
            return std::string_view{token.value().data() + 1, token.value().size() - 2};
        }
        return token.value();
    }

} // namespace components::sql
