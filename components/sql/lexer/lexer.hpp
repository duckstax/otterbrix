#pragma once

#include "token.hpp"
#include <string>

namespace components::sql {

    class lexer_t {
    public:
        lexer_t(const char* const query_begin, const char* const query_end);
        explicit lexer_t(std::string_view query);
        explicit lexer_t(const std::string& query);

        token_t next_not_whitespace_token();
        token_t next_token();
        token_t current_token() const;
        token_t current_significant_token() const;
        void save();
        void restore();

    private:
        const char* const begin_;
        const char* const end_;
        const char* pos_;
        const char* saved_pos_;
        token_t prev_token_;
        token_t prev_significant_token_;

        token_t next_token_();
        inline bool check_pos_(char c);
        inline token_t
        create_quote_token_(const char* const token_begin, char quote, token_type type, token_type type_error);
        inline token_t create_comment_one_line_(const char* const token_begin);
        inline token_t create_comment_multi_line_(const char* const token_begin);
        inline token_t create_hex_or_bin_str(const char* const token_begin);
    };

} // namespace components::sql
