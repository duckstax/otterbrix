#include <catch2/catch.hpp>
#include <components/sql/lexer/lexer.hpp>

using namespace components::sql;

#define CHECK_NEXT_TOKEN_TYPE(LEXER, TYPE) { \
    auto token = LEXER.next_token();  \
    REQUIRE(token.type == TYPE); \
    }

#define CHECK_NEXT_TOKEN(LEXER, TYPE, VALUE) { \
    auto token = LEXER.next_token();  \
    REQUIRE(token.type == TYPE); \
    REQUIRE(token.value() == VALUE); \
    }


TEST_CASE("lexer::base") {
    std::string query{"SELECT * FROM table;"};
    lexer_t lexer{query};
    CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
    CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
    CHECK_NEXT_TOKEN(lexer, token_type::asterisk, "*");
    CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
    CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
    CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
    CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
    CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
    CHECK_NEXT_TOKEN_TYPE(lexer, token_type::end_query);
}
