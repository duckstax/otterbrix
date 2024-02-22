#pragma once

#include "parse_error.hpp"
#include <components/sql/lexer/token.hpp>
#include <string>

namespace components::sql::impl {

    struct parser_result {
        bool finished;
        parse_error error;
        std::string what;
        token_t error_token;

        parser_result(bool finished);
        parser_result(parse_error error, const token_t& error_token, const std::string& what = std::string());

        explicit operator bool() const;
        bool is_error() const;
    };

} // namespace components::sql::impl
