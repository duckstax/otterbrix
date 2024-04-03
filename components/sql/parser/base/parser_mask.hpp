#pragma once

#include "parser_result.hpp"
#include <components/document/wrapper_value.hpp>
#include <components/sql/lexer/lexer.hpp>
#include <components/sql/lexer/token.hpp>
#include <string>
#include <vector>

namespace components::sql::impl {

    struct mask_element_t {
        token_type type{token_type::bare_word};
        std::string value;
        bool optional{false};
        bool is_value{false};

        mask_element_t(token_type type, const std::string& value, bool optional = false);

        static mask_element_t create_value_mask_element();
        static mask_element_t create_optional_value_mask_element();
    };

    bool operator==(const mask_element_t& elem, const token_t& token);
    bool operator!=(const mask_element_t& elem, const token_t& token);

    struct mask_group_element_t {
        enum class status
        {
            yes,
            no,
            error
        };

        std::vector<mask_element_t> words;

        explicit mask_group_element_t(const std::vector<std::string>& words);

        status check(lexer_t& lexer) const;
    };

    class mask_t {
    public:
        explicit mask_t(const std::vector<mask_element_t>& elements);

        bool match(lexer_t& lexer);
        std::string cap(std::size_t index) const;

    private:
        std::vector<mask_element_t> elements_;
    };

    bool contains_mask_element(lexer_t& lexer, const mask_element_t& elem);

    ::document::wrapper_value_t parse_value(const token_t& token);
    parser_result parse_field_names(lexer_t& lexer, std::pmr::vector<std::string>& fields);
    parser_result parse_field_values(lexer_t& lexer, std::pmr::vector<::document::wrapper_value_t>& values);

} // namespace components::sql::impl
