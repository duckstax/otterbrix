#include "parser_mask.h"
#include <numeric>
#include <set>

namespace components::sql::impl {

    namespace  {

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
            static const std::set<token_type> compare_value_types {
                token_type::bare_word
            };

            return elem.type == token.type
                && (compare_value_types.find(elem.type) == compare_value_types.end()
                    || compare_str_case_insensitive(elem.value, token.value()));
        }

    } // namespace

    mask_element_t::mask_element_t(token_type type, const std::string& value, bool optional)
        : type(type)
        , value(value)
        , optional(optional) {
    }

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


    mask_t::mask_t(const std::vector<mask_element_t>& elements)
        : elements_(elements) {
    }

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

    std::string mask_t::cap(std::size_t index) const {
        return elements_.at(index).value;
    }


    bool contents_mask_element(lexer_t& lexer, const mask_element_t& elem) {
        auto token = lexer.next_token();
        while (!is_token_end_query(token)) {
            if (equals(elem, token)) {
                return true;
            }
            token = lexer.next_token();
        }
        return false;
    }

} // namespace components::sql::impl
