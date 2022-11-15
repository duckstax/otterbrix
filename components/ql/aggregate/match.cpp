#include "match.hpp"
#include <sstream>

namespace components::ql::aggregate {

    match_t make_match(expr_ptr &&query) {
        match_t match;
        match.query = std::move(query);
        return match;
    }

#ifdef DEV_MODE
    std::string debug(const match_t &match) {
        std::stringstream stream;
        stream << match;
        return stream.str();
    }
#endif

} // namespace components::ql::aggregate