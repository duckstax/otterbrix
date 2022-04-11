#include <catch2/catch.hpp>
#include <oid/hex.hpp>
#include <cstring>

TEST_CASE("is_hex") {
    REQUIRE(is_hex('0'));
    REQUIRE(is_hex('1'));
    REQUIRE(is_hex('9'));
    REQUIRE(is_hex('a'));
    REQUIRE(is_hex('f'));
    REQUIRE(is_hex('A'));
    REQUIRE(is_hex('F'));
    REQUIRE_FALSE(is_hex('t'));
}

TEST_CASE("char_to_hex") {
    char hex[2];
    char_to_hex(char(0x10), hex);
    REQUIRE(std::string_view(hex, 2) == "10");
    char_to_hex(char(0xf5), hex);
    REQUIRE(std::string_view(hex, 2) == "f5");
    char_to_hex(char(0x00), hex);
    REQUIRE(std::string_view(hex, 2) == "00");
    char_to_hex(char(0xff), hex);
    REQUIRE(std::string_view(hex, 2) == "ff");
}

TEST_CASE("hex_to_char") {
    char c;
    hex_to_char("10", c);
    REQUIRE(c == char(0x10));
    hex_to_char("f5", c);
    REQUIRE(c == char(0xf5));
    hex_to_char("00", c);
    REQUIRE(c == char(0x00));
    hex_to_char("ff", c);
    REQUIRE(c == char(0xff));
}
