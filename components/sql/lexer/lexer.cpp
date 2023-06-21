#include "lexer.hpp"

namespace components::sql {

    lexer_t::lexer_t(const char* const query_begin, const char* const query_end)
        : begin_(query_begin)
        , end_(query_end)
        , pos_(begin_) {
    }

    lexer_t::lexer_t(std::string_view query)
        : lexer_t(query.data(), query.data() + query.size()) {
    }

    token_t lexer_t::next_token() {
        //todo: impl
        return token_t{};
    }

} // namespace components::sql
