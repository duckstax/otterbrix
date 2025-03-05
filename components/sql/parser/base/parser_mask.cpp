#include "parser_mask.hpp"
#include <algorithm>
#include <charconv>
#include <memory_resource>
#include <numeric>
#include <set>
#include <string>

namespace components::sql::impl {

    namespace {

        inline bool compare_str_case_insensitive(const std::string& s1, std::string_view s2) {
            if (s1.size() != s2.size()) {
                return false;
            }
            auto it2 = s2.begin();
            for (auto it1 = s1.begin(); it1 < s1.end(); ++it1, ++it2) {
                if (std::tolower(*it1) != std::tolower(*it2)) {
                    return false;
                }
            }
            return true;
        }

        inline bool equals(const mask_element_t& elem, const token_t& token) {
            static const std::set<token_type> compare_value_types{token_type::bare_word};

            return elem.type == token.type && (compare_value_types.find(elem.type) == compare_value_types.end() ||
                                               compare_str_case_insensitive(elem.value, token.value()));
        }

        inline bool is_integer(std::string_view data) {
            auto it = std::find(data.begin(), data.end(), '.');
            return it == data.end();
        }

    } // namespace

    mask_element_t::mask_element_t(token_type type, const std::string& value, bool optional)
        : type(type)
        , value(value)
        , optional(optional) {}

    mask_element_t mask_element_t::create_value_mask_element() {
        mask_element_t elem{token_type::bare_word, ""};
        elem.is_value = true;
        return elem;
    }

    mask_element_t mask_element_t::create_optional_value_mask_element() {
        mask_element_t elem{token_type::bare_word, "", true};
        elem.is_value = true;
        return elem;
    }

    bool operator==(const mask_element_t& elem, const token_t& token) { return equals(elem, token); }

    bool operator!=(const mask_element_t& elem, const token_t& token) { return !(elem == token); }

    mask_group_element_t::mask_group_element_t(const std::vector<std::string>& words) {
        this->words.reserve(words.size());
        std::transform(words.begin(), words.end(), std::back_inserter(this->words), [](const std::string& word) {
            return mask_element_t{token_type::bare_word, word};
        });
    }

    mask_group_element_t::status mask_group_element_t::check(lexer_t& lexer) const {
        if (words.front() != lexer.current_significant_token()) {
            return status::no;
        }
        for (auto it = words.begin() + 1; it != words.end(); ++it) {
            auto token = lexer.next_not_whitespace_token();
            if (*it != token) {
                return status::error;
            }
        }
        return status::yes;
    }

    mask_t::mask_t(const std::vector<mask_element_t>& elements)
        : elements_(elements) {}

    bool mask_t::match(lexer_t& lexer) {
        auto token = lexer.next_token();
        for (auto it = elements_.begin(); it < elements_.end(); ++it) {
            while (token.type == token_type::comment) {
                token = lexer.next_token();
                if (token.type == token_type::whitespace && it->type != token_type::whitespace) {
                    token = lexer.next_token();
                }
            }

            if (is_token_end(token) && it != elements_.end() && !it->optional) {
                return false;
            }

            if (it->is_value) {
                if (token.type == token_type::bare_word) {
                    it->value = token.value();
                }
            }

            if (!equals(*it, token) && !it->optional) {
                return false;
            }

            if (equals(*it, token)) {
                token = lexer.next_token();
            }
        }
        return true;
    }

    std::string mask_t::cap(std::size_t index) const { return elements_.at(index).value; }

    bool contains_mask_element(lexer_t& lexer, const mask_element_t& elem) {
        auto token = lexer.next_token();
        while (!is_token_end_query(token)) {
            if (equals(elem, token)) {
                return true;
            }
            token = lexer.next_token();
        }
        return false;
    }

    document::value_t parse_value(const token_t& token, document::impl::base_document* tape) {
        if (token.type == token_type::string_literal) {
            return document::value_t(tape, token_clean_value(token));
        } else if (token.type == token_type::number_literal) {
            if (is_integer(token.value())) {
                int64_t result;
                auto [ptr, ec] =
                    std::from_chars(token.value().data(), token.value().data() + token.value().size(), result);
                return document::value_t(tape, result);
            } else {
                // TODO we don't support float in this case?
                return document::value_t(tape, std::atof(token.value().data()));
            }
        } else if (is_token_bool_value_true(token)) {
            return document::value_t(tape, true);
        } else if (is_token_bool_value_false(token)) {
            return document::value_t(tape, false);
        }
        return document::value_t(tape, nullptr);
    }

    parser_result parse_field_names(lexer_t& lexer, std::pmr::vector<std::string>& fields) {
        auto token = lexer.next_not_whitespace_token();
        if (token.type != token_type::bracket_round_open) {
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid fields list"};
        }
        token = lexer.next_not_whitespace_token();
        while (token.type != token_type::bracket_round_close && !is_token_end_query(token)) {
            if (!is_token_field_name(token)) {
                return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid fields list"};
            }
            fields.emplace_back(token_clean_value(token));
            token = lexer.next_not_whitespace_token();
            if (token.type != token_type::comma && token.type != token_type::bracket_round_close) {
                return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid fields list"};
            }
            if (token.type == token_type::bracket_round_close) {
                break;
            }
            token = lexer.next_not_whitespace_token();
        }
        if (is_token_end_query(token)) {
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid fields list"};
        }
        if (fields.empty()) {
            return components::sql::impl::parser_result{parse_error::empty_fields_list, token, "empty fields list"};
        }
        return true;
    }

    parser_result parse_field_values(lexer_t& lexer,
                                     std::pmr::vector<document::value_t>& values,
                                     document::impl::base_document* tape) {
        values.clear();
        auto token = lexer.next_not_whitespace_token();
        if (token.type != token_type::bracket_round_open) {
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid values list"};
        }
        token = lexer.next_not_whitespace_token();
        while (token.type != token_type::bracket_round_close && !is_token_end_query(token)) {
            if (!is_token_field_value(token)) {
                return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid values list"};
            }
            values.push_back(parse_value(token, tape));
            token = lexer.next_not_whitespace_token();
            if (token.type != token_type::comma && token.type != token_type::bracket_round_close) {
                return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid values list"};
            }
            if (token.type == token_type::bracket_round_close) {
                break;
            }
            token = lexer.next_not_whitespace_token();
        }
        if (is_token_end_query(token)) {
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid values list"};
        }
        if (values.empty()) {
            return components::sql::impl::parser_result{parse_error::empty_values_list, token, "empty values list"};
        }
        return true;
    }

} // namespace components::sql::impl
