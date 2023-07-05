#pragma once

#include <string>

namespace components::sql {

    enum class parse_error {
        no_error,
        syntax_error,
        empty_fields_list,
        empty_values_list,
        not_valid_size_values_list
    };

    class error_t {
    public:
        error_t(parse_error error, std::string_view mistake, const std::string& what = std::string());

        explicit operator bool() const;
        parse_error error() const;
        std::string_view mistake() const;
        std::string_view what() const;

    private:
        const parse_error error_;
        const std::string_view mistake_;
        const std::string what_;
    };

} // namespace components::sql
