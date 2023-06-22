#include "lexer.hpp"
#include <cctype>

namespace components::sql {

    lexer_t::lexer_t(const char* const query_begin, const char* const query_end)
        : begin_(query_begin)
        , end_(query_end)
        , pos_(begin_) {
    }

    lexer_t::lexer_t(std::string_view query)
        : lexer_t(query.data(), query.data() + query.size()) {
    }

    token_t lexer_t::next_token() {
        if (pos_ >= end_) {
            return token_t{token_type::end_query};
        }
        auto token = next_token_();
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
            //todo: impl
        }

        if (*pos_ == '\'') {
            //todo: impl find closed quote
        }

        if (*pos_ == '"') {
            //todo: impl find closed quote
        }

        if (*pos_ == '`') {
            //todo: impl find closed quote
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
            //todo: impl
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
                //todo: impl find \n to comment
                return token_t{token_type::comment, token_begin, ++pos_};
            }
            return token_t{token_type::minus, token_begin, pos_};
        }

        if (*pos_ == '*') {
            return token_t{token_type::asterisk, token_begin, ++pos_};
        }

        if (*pos_ == '/') {
            ++pos_;
            if (check_pos_('/')) {
                //todo: impl find \n to comment
            }
            if (check_pos_('*')) {
                //todo: impl find */ to comment
            }
            return token_t{token_type::slash, token_begin, pos_};
        }

        if (*pos_ == '#') {
            ++pos_;
            if (check_pos_(' ')) {
                //todo: impl find \n to comment
            }
            if (check_pos_('!')) {
                //todo: impl find \n to comment
            }
            return token_t{token_type::error};
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
            return token_t{token_type::error_single_exclamation_mark};
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
            return token_t{token_type::error};
        }

        if (*pos_ == '$') {
            //todo: impl
        }

        //todo: impl other variants

        return token_t{};
    }

    bool lexer_t::check_pos_(char c) {
        return pos_ < end_ && *pos_ == c;
    }

} // namespace components::sql
