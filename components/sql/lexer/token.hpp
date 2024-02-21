#pragma once

#include <string_view>

namespace components::sql {

    enum class token_type {
        unknow,
        whitespace,
        comment,
        doc,

        bare_word,
        number_literal,
        string_literal,
        quoted_identifier,

        bracket_round_open,
        bracket_round_close,
        bracket_square_open,
        bracket_square_close,
        bracket_curly_open,
        bracket_curly_close,

        dot,
        comma,
        semicolon,
        colon,
        double_colon,
        vertical_delimiter,

        plus,
        minus,
        asterisk,
        slash,
        percent,
        dollar,
        question,
        at,
        double_at,
        arrow,
        concatenation,
        pipe_mark,
        equals,
        not_equals,
        less,
        greater,
        less_or_equals,
        greater_or_equals,

        end_line,
        end_query,

        error,
        error_multiline_comment_is_not_closed,
        error_single_quote_is_not_closed,
        error_double_quote_is_not_closed,
        error_back_quote_is_not_closed,
        error_single_exclamation_mark,
        error_wrong_number
    };

    struct token_t {
        token_type type{token_type::unknow};
        const char* begin{nullptr};
        const char* end{nullptr};

        token_t() = default;
        token_t(token_type type, const char* begin, const char* end);
        explicit token_t(token_type type);

        std::size_t size() const;
        std::string_view value() const;
    };

    bool operator==(const token_t& token1, const token_t& token2);
    bool operator<(const token_t& token1, const token_t& token2);

    bool is_token_significant(const token_t& token);
    bool is_token_error(const token_t& token);
    bool is_token_end(const token_t& token);
    bool is_token_end_query(const token_t& token);
    bool is_token_bool_value_true(const token_t& token);
    bool is_token_bool_value_false(const token_t& token);
    bool is_token_bool_value(const token_t& token);
    bool is_token_field_name(const token_t& token);
    bool is_token_field_value(const token_t& token);

    std::string_view token_name(token_type type);
    std::string_view token_name(const token_t& token);

    std::string_view token_clean_value(const token_t& token);

} // namespace components::sql
