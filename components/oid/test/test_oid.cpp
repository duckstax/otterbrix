#include <catch2/catch.hpp>
#include <oid/oid.hpp>

using namespace oid;

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
    using oid1_t = oid_t<8>;

    REQUIRE(oid1_t("0000000000000000") == oid1_t::null());
    REQUIRE(oid1_t::null() < oid1_t("0000000000000001"));
    REQUIRE(oid1_t("fffffffffffffffe") < oid1_t::max());
    REQUIRE(oid1_t("ffffffffffffffff") == oid1_t::max());
}

TEST_CASE("oid::generate by time") {
    using oid12_t = oid_t<12>;
    REQUIRE(oid12_t(0x1).get_timestamp() == 0x1);
    REQUIRE(oid12_t(0xffff).get_timestamp() == 0xffff);
    REQUIRE(oid12_t(0x12345678).get_timestamp() == 0x12345678);
    REQUIRE(oid12_t(0xffffffff).get_timestamp() == 0xffffffff);
}

//#include <iostream>
//TEST_CASE("oid::generate") {
//    using oid12_t = oid_t<12>;
//    for (uint i = 0; i < 10; ++i) {
//        std::cerr << "~~~ GENERATING N" << i + 1 << " ~~~" << std::endl;
//        for (uint j = 0; j < 100; ++j) {
//            std::cerr << oid12_t() << std::endl;
//        }
//    }
//}
