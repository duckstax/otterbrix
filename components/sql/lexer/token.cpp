#include "token.hpp"
#include <magic_enum.hpp>

namespace components::sql {

    token_t::token_t(token_type type, const char* begin, const char* end)
        : type(type)
        , begin(begin)
        , end(end) {
    }

    token_t::token_t(token_type type)
        : token_t(type, nullptr, nullptr) {
    }

    std::size_t token_t::size() const {
        return end - begin;
    }


    bool is_token_significant(const token_t& token) {
        return token.type != token_type::whitespace
            && token.type != token_type::comment;
    }

    bool is_token_error(const token_t& token) {
        return token.type >= token_type::error;
    }

    bool is_token_end(const token_t& token) {
        return token.type == token_type::end_query;
    }

    std::string_view token_name(token_type type) {
        return magic_enum::enum_name(type);
    }

    std::string_view token_name(const token_t& token) {
        return token_name(token.type);
    }

} // namespace components::sql
