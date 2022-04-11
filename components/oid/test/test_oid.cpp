#include <catch2/catch.hpp>
#include <oid/oid.hpp>

TEST_CASE("oid::is_valid") {
    using oid8_t = oid_t<8>;

    REQUIRE(oid8_t::is_valid("0123456789abcdef"));
    REQUIRE_FALSE(oid8_t::is_valid("0123456789abcde"));
    REQUIRE_FALSE(oid8_t::is_valid("0123456789abcdef0"));
    REQUIRE_FALSE(oid8_t::is_valid("0123456789abcdeg"));
}

TEST_CASE("oid::initialization") {
    using oid8_t = oid_t<8>;

    REQUIRE(oid8_t("0123456789abcdef").to_string() == "0123456789abcdef");
    REQUIRE(oid8_t("0123456789abcde").to_string() == "0000000000000000");
    REQUIRE(oid8_t::null().to_string() == "0000000000000000");
    REQUIRE(oid8_t::max().to_string() == "ffffffffffffffff");
}

TEST_CASE("oid::operators") {
    using oid1_t = oid_t<1>;

    REQUIRE(oid1_t("00") == oid1_t::null());
    REQUIRE(oid1_t::null() < oid1_t("01"));
    REQUIRE(oid1_t("01") < oid1_t::max());
}
