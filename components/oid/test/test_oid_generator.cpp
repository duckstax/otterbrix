#include <catch2/catch.hpp>
#include <oid/oid_generator.hpp>

TEST_CASE("oid_generator::generate") {
    using oid8_t = oid_t<12>;
    oid_generator_t<4, 5, 3> generator(oid8_t("0123456701234567891000f8"));
    REQUIRE(generator.next() == oid8_t("0123456701234567891000f9"));
    REQUIRE(generator.next() == oid8_t("0123456701234567891000fa"));
    REQUIRE(generator.next() == oid8_t("0123456701234567891000fb"));
    REQUIRE(generator.next() == oid8_t("0123456701234567891000fc"));
    REQUIRE(generator.next() == oid8_t("0123456701234567891000fd"));
    REQUIRE(generator.next() == oid8_t("0123456701234567891000fe"));
    REQUIRE(generator.next() == oid8_t("0123456701234567891000ff"));
    REQUIRE(generator.next() == oid8_t("012345670123456789100100"));
    REQUIRE(generator.next() == oid8_t("012345670123456789100101"));
}

//#include <iostream>
//TEST_CASE("oid_generator") {
//    for (uint i = 0; i < 10; ++i) {
//        std::cerr << "~~~ GENERATING N" << i + 1 << " ~~~" << std::endl;
//        oid_generator_t<4, 5, 3> generator;
//        for (uint j = 0; j < 100; ++j) {
//            std::cerr << generator.next() << std::endl;
//        }
//    }
//}
