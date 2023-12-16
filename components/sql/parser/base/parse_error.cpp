#include "parse_error.hpp"

namespace components::sql {

    error_t::error_t(parse_error error, std::string_view mistake, const std::string& what)
        : error_(error)
        , mistake_(mistake)
        , what_(what) {}

    components::sql::error_t::operator bool() const { return error_ > parse_error::no_error; }

    parse_error error_t::error() const { return error_; }

    std::string_view error_t::mistake() const { return mistake_; }

    std::string_view error_t::what() const { return std::string_view(what_); }

} // namespace components::sql
