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

TEST_CASE("oid_generator::generate by time") {
    using generator_t = oid_generator_t<4, 5, 3>;
    {
        generator_t generator(0x1);
        REQUIRE(generator.get().to_string().substr(0, 8) == "00000001");
    }
    {
        generator_t generator(0xffff);
        REQUIRE(generator.get().to_string().substr(0, 8) == "0000ffff");
    }
    {
        generator_t generator(0x12345678);
        REQUIRE(generator.get().to_string().substr(0, 8) == "12345678");
    }
    {
        generator_t generator(0xffffffff);
        REQUIRE(generator.get().to_string().substr(0, 8) == "ffffffff");
    }
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
