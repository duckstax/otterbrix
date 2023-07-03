#include "parser_result.hpp"
#include <cassert>

namespace components::sql {

    parser_result::parser_result(bool finished)
        : finished(finished)
        , error(parse_error::no_error) {
    }

    parser_result::parser_result(parse_error error, const token_t& error_token, const std::string& what)
        : finished(true)
        , error(error)
        , what(what)
        , error_token(error_token) {
        assert(error != parse_error::no_error);
    }

    bool parser_result::is_error() const {
        return error > parse_error::no_error;
    }

    parser_result::operator bool() const {
        return finished;
    }

} // namespace components::sql
