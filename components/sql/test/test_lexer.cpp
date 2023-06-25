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

    SECTION("word|*|;") {
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

    SECTION("word|*|.|;") {
        std::string query{"SELECT * FROM schema.table;"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::asterisk, "*");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "schema");
        CHECK_NEXT_TOKEN(lexer, token_type::dot, ".");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN_TYPE(lexer, token_type::end_query);
    }

    SECTION("quote") {
        std::string query{"SELECT id, 'string' AS `str` FROM \"schema\".\"table\";"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "id");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::string_literal, "'string'");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "AS");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::quoted_identifier, "`str`");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::quoted_identifier, "\"schema\"");
        CHECK_NEXT_TOKEN(lexer, token_type::dot, ".");
        CHECK_NEXT_TOKEN(lexer, token_type::quoted_identifier, "\"table\"");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN_TYPE(lexer, token_type::end_query);
    }

    SECTION("quote|escape") {
        std::string query{"SELECT id, 'string: \\'Hello\\'' AS `str` FROM \"schema\".\"table\";"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "id");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::string_literal, "'string: \\'Hello\\''");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "AS");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::quoted_identifier, "`str`");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::quoted_identifier, "\"schema\"");
        CHECK_NEXT_TOKEN(lexer, token_type::dot, ".");
        CHECK_NEXT_TOKEN(lexer, token_type::quoted_identifier, "\"table\"");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN_TYPE(lexer, token_type::end_query);
    }

}


TEST_CASE("lexer::comments") {

    SECTION("-- ") {
        std::string query{"SELECT * FROM table; -- comments text"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::asterisk, "*");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::comment, "-- comments text");
        CHECK_NEXT_TOKEN_TYPE(lexer, token_type::end_query);
    }

    SECTION("//") {
        std::string query{"SELECT * FROM table; --comments text"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::asterisk, "*");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::comment, "--comments text");
        CHECK_NEXT_TOKEN_TYPE(lexer, token_type::end_query);
    }

    SECTION("-- multiline") {
        std::string query{"SELECT * FROM table; -- comments text\n"
                          "COMMIT;"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::asterisk, "*");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::comment, "-- comments text");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, "\n");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "COMMIT");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN_TYPE(lexer, token_type::end_query);
    }

    SECTION("// ") {
        std::string query{"SELECT * FROM table; // comments text"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::asterisk, "*");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::comment, "// comments text");
        CHECK_NEXT_TOKEN_TYPE(lexer, token_type::end_query);
    }

    SECTION("//") {
        std::string query{"SELECT * FROM table; //comments text"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::asterisk, "*");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::comment, "//comments text");
        CHECK_NEXT_TOKEN_TYPE(lexer, token_type::end_query);
    }

    SECTION("// multiline") {
        std::string query{"SELECT * FROM table; // comments text\n"
                          "COMMIT;"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::asterisk, "*");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::comment, "// comments text");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, "\n");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "COMMIT");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN_TYPE(lexer, token_type::end_query);
    }

    SECTION("# ") {
        std::string query{"SELECT * FROM table; # comments text"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::asterisk, "*");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::comment, "# comments text");
        CHECK_NEXT_TOKEN_TYPE(lexer, token_type::end_query);
    }

    SECTION("#!") {
        std::string query{"SELECT * FROM table; #!comments text"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::asterisk, "*");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::comment, "#!comments text");
        CHECK_NEXT_TOKEN_TYPE(lexer, token_type::end_query);
    }

    SECTION("# error") {
        std::string query{"SELECT * FROM table; #error comments text"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::asterisk, "*");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN_TYPE(lexer, token_type::error);
    }

    SECTION("/*") {
        std::string query{"SELECT * FROM table; /* comments text */"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::asterisk, "*");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::comment, "/* comments text */");
        CHECK_NEXT_TOKEN_TYPE(lexer, token_type::end_query);
    }

    SECTION("/* multiline") {
        std::string query{"SELECT * FROM table; /* multiline\n"
                          "comments text */\n"
                          "COMMIT;"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::asterisk, "*");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::comment, "/* multiline\n"
                                                     "comments text */");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, "\n");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "COMMIT");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN_TYPE(lexer, token_type::end_query);
    }

    SECTION("/* multiline error") {
        std::string query{"SELECT * FROM table; /* multiline\n"
                          "comments text\n"
                          "COMMIT;"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::asterisk, "*");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN_TYPE(lexer, token_type::error_multiline_comment_is_not_closed);
    }

}
