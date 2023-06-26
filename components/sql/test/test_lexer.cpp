#include <catch2/catch.hpp>
#include <components/sql/lexer/lexer.hpp>

using namespace components::sql;

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
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
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
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
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
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
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
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
    }

}


TEST_CASE("lexer::digits") {

    SECTION("simple") {
        std::string query{"SELECT 1, 2 FROM table;"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, "1");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, "2");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
    }

    SECTION("sign|exponent") {
        std::string query{"SELECT 123456789, 0.123456789, 0.12e12, 1.23E-567, 1.23E+567, "
                          "-123456789, -0.123456789, -0.12e12, -1.23E-567, -1.23E+567 FROM table;"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, "123456789");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, "0.123456789");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, "0.12e12");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, "1.23E-567");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, "1.23E+567");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::minus, "-");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, "123456789");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::minus, "-");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, "0.123456789");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::minus, "-");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, "0.12e12");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::minus, "-");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, "1.23E-567");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::minus, "-");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, "1.23E+567");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
    }

    SECTION("hex|bin") {
        std::string query{"SELECT 0x123fed, 0X12345fed, 0b1010, 0B1010 FROM table;"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, "0x123fed");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, "0X12345fed");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, "0b1010");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, "0B1010");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
    }

    SECTION("error") {
        std::string query{"SELECT 123f FROM table;"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::error_wrong_number, "123f");
    }

    SECTION("hex error") {
        std::string query{"SELECT 0x123fedr FROM table;"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::error_wrong_number, "0x123fedr");
    }

    SECTION("bin error") {
        std::string query{"SELECT 0b123 FROM table;"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::error_wrong_number, "0b123");
    }

    SECTION("first char = .") {
        std::string query{"SELECT .123456789, .12e12, .23E-567, .23E+567, "
                          "-.123456789, -.12e12, -.23E-567, -.23E+567 FROM table;"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, ".123456789");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, ".12e12");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, ".23E-567");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, ".23E+567");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::minus, "-");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, ".123456789");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::minus, "-");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, ".12e12");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::minus, "-");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, ".23E-567");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::minus, "-");
        CHECK_NEXT_TOKEN(lexer, token_type::number_literal, ".23E+567");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
    }

    SECTION("first char = . error") {
        std::string query{"SELECT .123f FROM table;"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::error_wrong_number, ".123f");
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
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
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
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
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
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
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
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
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
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
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
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
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
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
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
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
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
        CHECK_NEXT_TOKEN(lexer, token_type::error, "#error comments text");
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
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
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
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
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
        CHECK_NEXT_TOKEN(lexer, token_type::error_multiline_comment_is_not_closed,
                         "/* multiline\n"
                          "comments text\n"
                          "COMMIT;");
    }

    SECTION("include /*") {
        std::string query{"SELECT * FROM table; /* comment /* include comment */ ... */"};
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
        CHECK_NEXT_TOKEN(lexer, token_type::comment, "/* comment /* include comment */ ... */");
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
    }

    SECTION("include /* error") {
        std::string query{"SELECT * FROM table; /* comment /* include comment ... */"};
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
        CHECK_NEXT_TOKEN(lexer, token_type::error_multiline_comment_is_not_closed, "/* comment /* include comment ... */");
    }

    SECTION("hex|bin string literal") {
        std::string query{"SELECT x'123abc', b'10101010' FROM table;"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::string_literal, "x'123abc'");
        CHECK_NEXT_TOKEN(lexer, token_type::comma, ",");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::string_literal, "b'10101010'");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "FROM");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "table");
        CHECK_NEXT_TOKEN(lexer, token_type::semicolon, ";");
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
    }

    SECTION("hex string literal error digit") {
        std::string query{"SELECT x'123arbc' FROM table;"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::error_single_quote_is_not_closed, "x'123ar");
    }

    SECTION("hex string literal error digit") {
        std::string query{"SELECT b'101012010' FROM table;"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::error_single_quote_is_not_closed, "b'101012");
    }

    SECTION("hex string literal error digit") {
        std::string query{"SELECT b'10101010 FROM table;"};
        lexer_t lexer{query};
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "SELECT");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::error_single_quote_is_not_closed, "b'10101010 ");
    }

    SECTION("$doc$") {
        std::string query{"SELECT * FROM table; $ docs to ... $"};
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
        CHECK_NEXT_TOKEN(lexer, token_type::doc, "$ docs to ... $");
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
    }

    SECTION("$ endl") {
        std::string query{"SELECT * FROM table; $"};
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
        CHECK_NEXT_TOKEN(lexer, token_type::dollar, "$");
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
    }

    SECTION("$ midline") {
        std::string query{"SELECT * FROM table; $ docs to ..."};
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
        CHECK_NEXT_TOKEN(lexer, token_type::dollar, "$");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "docs");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::bare_word, "to");
        CHECK_NEXT_TOKEN(lexer, token_type::whitespace, " ");
        CHECK_NEXT_TOKEN(lexer, token_type::dot, ".");
        CHECK_NEXT_TOKEN(lexer, token_type::dot, ".");
        CHECK_NEXT_TOKEN(lexer, token_type::dot, ".");
        CHECK_NEXT_TOKEN(lexer, token_type::end_query, "");
    }

    SECTION("$ midline") {
        std::string query{"SELECT * FROM table; $docs to ..."};
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
        CHECK_NEXT_TOKEN(lexer, token_type::error, "");
    }

}
