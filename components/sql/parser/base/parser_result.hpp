#pragma once

#include <string>

namespace components::sql {

    enum class parse_error {
        no_error,
        syntax_error
    };

    struct parser_result
    {
        bool finished;
        parse_error error;
        std::string what;

        parser_result(bool finished);
        explicit parser_result(parse_error error, const std::string& what = std::string());

        explicit operator bool() const;
        bool is_error() const;
    };

} // namespace components::sql
