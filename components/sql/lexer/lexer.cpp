#include "lexer.hpp"
#include <cctype>
#include <algorithm>
#include <set>

namespace components::sql {

    namespace {

        enum class number_base {
            dec,
            hex,
            bin
        };


        inline bool is_word_char(char c) {
            return c == '_' || std::isalnum(c);
        }

        inline bool is_binary_digit(char c) {
            return c == '0' || c == '1';
        }

        inline bool is_digit(char c, number_base base) {
            return base == number_base::dec
                    ? std::isdigit(c)
                    : base == number_base::hex
                      ? std::isxdigit(c)
                      : is_binary_digit(c);
        }

        inline bool is_exponent(char c, number_base base) {
            return base == number_base::hex
                    ? (c == 'p' || c == 'P')
                    : (c == 'e' || c == 'E');
        }

        inline bool is_sign(char c) {
            return c == '-' || c == '+';
        }

        inline bool is_number_base_hex(char c) {
            return c == 'x' || c == 'X';
        }

        inline bool is_number_base_bin(char c) {
            return c == 'b' || c == 'B';
        }

        inline bool is_number_separator(const char* pos, const char* end, bool is_block_begin,
                                        number_base base = number_base::dec) {
            if (*pos != '_') {
                return false;
            }
            if (is_block_begin) {
                return false;
            }
            auto next = pos + 1;
            if (next >= end) {
                return false;
            }
            return is_digit(*next, base);
        }

        inline const char* find_char(const char* begin, const char* end, char c) {
            auto it = std::find(begin, end, c);
            return it;
        }

        inline const char* find_quote(const char* begin, const char* end, char quote) {
            if (*begin == quote) {
                return begin;
            }
            for (auto it = begin + 1, prev = begin; it < end; ++it, ++prev) {
                if (*it == quote && *prev != '\\') {
                    return it;
                }
            }
            return end;
        }

    } // namespace


    lexer_t::lexer_t(const char* const query_begin, const char* const query_end)
        : begin_(query_begin)
        , end_(query_end)
        , pos_(begin_)
        , prev_significant_token_type_(token_type::unknow) {
    }

    lexer_t::lexer_t(std::string_view query)
        : lexer_t(query.data(), query.data() + query.size()) {
    }

    lexer_t::lexer_t(const std::string &query)
        : lexer_t(query.data(), query.data() + query.size()) {
    }

    token_t lexer_t::next_token() {
        if (pos_ >= end_) {
            return token_t{token_type::end_query};
        }
        auto token = next_token_();
        if (is_token_significant(token)) {
            prev_significant_token_type_ = token.type;
        }
        return token;
    }

    token_t lexer_t::next_token_() {
        const char* const token_begin = pos_;

        if (std::isspace(*pos_)) {
            do {
                ++pos_;
            } while (pos_ < end_ && std::isspace(*pos_));
            return token_t{token_type::whitespace, token_begin, pos_};
        }

        if (std::isdigit(*pos_)) {
            if (prev_significant_token_type_ == token_type::dot) {
                ++pos_;
                while (pos_ < end_ && (std::isdigit(*pos_) || is_number_separator(pos_, end_, false))) {
                    ++pos_;
                }
            } else {
                auto base = number_base::dec;
                if (pos_ + 1 < end_ && *pos_ == '0' && is_number_base_hex(*(pos_ + 1))) {
                    base = number_base::hex;
                    ++pos_;
                } else if (pos_ + 1 < end_ && *pos_ == '0' && is_number_base_bin(*(pos_ + 1))) {
                    base = number_base::bin;
                    ++pos_;
                }
                ++pos_;

                auto loop_digits = [&]() {
                    auto is_begin_block = false;
                    while (pos_ < end_ && (is_digit(*pos_, base) || is_number_separator(pos_, end_, is_begin_block, base))) {
                        ++pos_;
                        is_begin_block = *pos_ == '_';
                    }
                };

                loop_digits();

                if (pos_ < end_ && *pos_ == '.') {
                    ++pos_;
                    loop_digits();
                }

                if (pos_ < end_ && is_exponent(*pos_, base)) {
                    ++pos_;
                    if (pos_ < end_ && is_sign(*pos_)) {
                        ++pos_;
                    }
                    loop_digits();
                }

                if (!is_digit(*(pos_ - 1), base)) {
                    return token_t{token_type::error_wrong_number, token_begin, pos_};
                }
            }

            if (pos_ < end_ && std::isalpha(*pos_)) {
                while (pos_ < end_ && std::isalpha(*pos_)) {
                    ++pos_;
                }
                return token_t{token_type::error_wrong_number, token_begin, pos_};
            }
            return token_t{token_type::number_literal, token_begin, pos_};
        }

        if (*pos_ == '\'') {
            return create_quote_token_(token_begin, '\'', token_type::string_literal, token_type::error_single_quote_is_not_closed);
        }

        if (*pos_ == '"') {
            return create_quote_token_(token_begin, '"', token_type::quoted_identifier, token_type::error_double_quote_is_not_closed);
        }

        if (*pos_ == '`') {
            return create_quote_token_(token_begin, '`', token_type::quoted_identifier, token_type::error_back_quote_is_not_closed);
        }

        if (*pos_ == '(') {
            return token_t{token_type::bracket_round_open, token_begin, ++pos_};
        }
        if (*pos_ == ')') {
            return token_t{token_type::bracket_round_close, token_begin, ++pos_};
        }

        if (*pos_ == '[') {
            return token_t{token_type::bracket_square_open, token_begin, ++pos_};
        }
        if (*pos_ == ']') {
            return token_t{token_type::bracket_square_close, token_begin, ++pos_};
        }

        if (*pos_ == '{') {
            return token_t{token_type::bracket_curly_open, token_begin, ++pos_};
        }
        if (*pos_ == '}') {
            return token_t{token_type::bracket_curly_close, token_begin, ++pos_};
        }

        if (*pos_ == ',') {
            return token_t{token_type::comma, token_begin, ++pos_};
        }

        if (*pos_ == ';') {
            return token_t{token_type::semicolon, token_begin, ++pos_};
        }

        if (*pos_ == '.') {
            ++pos_;
            if (pos_ >= end_) {
                return token_t{token_type::dot, token_begin, pos_};
            }
            if (!std::isdigit(*pos_)) {
                return token_t{token_type::dot, token_begin, pos_};
            }
            static std::set<token_type> before_types{
                token_type::bracket_round_close,
                token_type::bracket_square_open,
                token_type::bare_word,
                token_type::quoted_identifier,
                token_type::number_literal
            };
            if (before_types.find(prev_significant_token_type_) != before_types.end()) {
                return token_t{token_type::dot, token_begin, pos_};
            }

            //todo: impl digit
        }

        if (*pos_ == '+') {
            return token_t{token_type::plus, token_begin, ++pos_};
        }

        if (*pos_ == '-') {
            ++pos_;
            if (check_pos_('>')) {
                return token_t{token_type::arrow, token_begin, ++pos_};
            }
            if (check_pos_('-')) {
                return create_comment_one_line_(token_begin);
            }
            return token_t{token_type::minus, token_begin, pos_};
        }

        if (*pos_ == '*') {
            return token_t{token_type::asterisk, token_begin, ++pos_};
        }

        if (*pos_ == '/') {
            ++pos_;
            if (check_pos_('/')) {
                return create_comment_one_line_(token_begin);
            }
            if (check_pos_('*')) {
                return create_comment_multi_line_(token_begin);
            }
            return token_t{token_type::slash, token_begin, pos_};
        }

        if (*pos_ == '#') {
            ++pos_;
            if (check_pos_(' ')) {
                return create_comment_one_line_(token_begin);
            }
            if (check_pos_('!')) {
                return create_comment_one_line_(token_begin);
            }
            return token_t{token_type::error, token_begin, pos_};
        }

        if (*pos_ == '%') {
            return token_t{token_type::percent, token_begin, ++pos_};
        }

        if (*pos_ == '=') {
            ++pos_;
            if (check_pos_('=')) {
                ++pos_;
            }
            return token_t{token_type::equals, token_begin, pos_};
        }

        if (*pos_ == '!') {
            ++pos_;
            if (check_pos_('=')) {
                return token_t{token_type::not_equals, token_begin, ++pos_};
            }
            return token_t{token_type::error_single_exclamation_mark,token_begin, pos_};
        }

        if (*pos_ == '<') {
            ++pos_;
            if (check_pos_('=')) {
                return token_t{token_type::less_or_equals, token_begin, ++pos_};
            }
            if (check_pos_('>')) {
                return token_t{token_type::not_equals, token_begin, ++pos_};
            }
            return token_t{token_type::less, token_begin, pos_};
        }

        if (*pos_ == '>') {
            ++pos_;
            if (check_pos_('=')) {
                return token_t{token_type::greater_or_equals, token_begin, ++pos_};
            }
            return token_t{token_type::greater, token_begin, pos_};
        }

        if (*pos_ == '?') {
            return token_t{token_type::question, token_begin, ++pos_};
        }

        if (*pos_ == ':') {
            ++pos_;
            if (check_pos_(':')) {
                return token_t{token_type::double_colon, token_begin, ++pos_};
            }
            return token_t{token_type::colon, token_begin, pos_};
        }

        if (*pos_ == '|') {
            ++pos_;
            if (check_pos_('|')) {
                return token_t{token_type::concatenation, token_begin, ++pos_};
            }
            return token_t{token_type::pipe_mark, token_begin, pos_};
        }

        if (*pos_ == '@') {
            ++pos_;
            if (check_pos_('@')) {
                return token_t{token_type::double_at, token_begin, ++pos_};
            }
            return token_t{token_type::at, token_begin, pos_};
        }

        if (*pos_ == '\\') {
            ++pos_;
            if (check_pos_('G')) {
                return token_t{token_type::vertical_delimiter, token_begin, ++pos_};
            }
            return token_t{token_type::error, token_begin, pos_};
        }

        if (*pos_ == '$') {
            //todo: impl
        }

        //todo: hex digits

        if (is_word_char(*pos_)) {
            ++pos_;
            while (pos_ < end_ && is_word_char(*pos_)) {
                ++pos_;
            }
            return token_t{token_type::bare_word, token_begin, pos_};
        }

        return token_t{token_type::error, token_begin, pos_};
    }

    bool lexer_t::check_pos_(char c) {
        return pos_ < end_ && *pos_ == c;
    }

    token_t lexer_t::create_quote_token_(const char* const token_begin, char quote, token_type type, token_type type_error) {
        pos_ = find_quote(++pos_, end_, quote);
        if (pos_ < end_) {
            return token_t{type, token_begin, ++pos_};
        }
        return token_t{type_error, token_begin, end_};
    }

    token_t lexer_t::create_comment_one_line_(const char* const token_begin) {
        pos_ = find_quote(++pos_, end_, '\n');
        return token_t{token_type::comment, token_begin, pos_};
    }

    token_t lexer_t::create_comment_multi_line_(const char* const token_begin) {
        //todo: include comments
        pos_ = find_char(++pos_, end_, '*');
        if (++pos_ < end_ && *pos_ == '/') {
            return token_t{token_type::comment, token_begin, ++pos_};
        }
        return token_t{token_type::error_multiline_comment_is_not_closed, token_begin, end_};
    }

} // namespace components::sql
