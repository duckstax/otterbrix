#include "match.hpp"

namespace components::ql::aggregate {

    match_t make_match(expressions::expression_ptr&& query) {
        match_t match;
        match.query = std::move(query);
        return match;
    }

} // namespace components::ql::aggregate