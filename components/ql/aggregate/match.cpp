#include "match.hpp"

namespace components::ql::aggregate {

    match_t make_match(expr_ptr &&query) {
        match_t match;
        match.query = std::move(query);
        return match;
    }

#ifdef DEV_MODE
    std::string debug(const match_t &match) {
        return std::string("$match: " + to_string(match.query));
    }
#endif

} // namespace components::ql::aggregate