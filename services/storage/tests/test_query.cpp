#include <catch2/catch.hpp>
#include "query.hpp"

using namespace services::storage;

TEST_CASE("condition_t") {

    SECTION("create") {
        auto c1 = condition_t<bool>([](bool v){ return v == true; });
        REQUIRE(c1.check(true));
        REQUIRE_FALSE(c1.check(false));

        auto c2 = condition_t<int>([](int v){ return v == 10; });
        REQUIRE(c2.check(10));
        REQUIRE_FALSE(c2.check(0));

        auto c3 = condition_t<std::string>([](std::string v){ return v == "test text"; });
        REQUIRE(c3.check("test text"));
        REQUIRE_FALSE(c3.check("testtext"));
        REQUIRE_FALSE(c3.check("Test text"));
    }

    SECTION("and or not") {
        auto c1 = condition_t<int>([](int v) { return v > 3; });
        auto c2 = condition_t<int>([](int v) { return v < 10; });

        REQUIRE((c1 & c2).check(5));
        REQUIRE_FALSE((c1 & c2).check(11));
        REQUIRE_FALSE((c1 & c2).check(2));

        REQUIRE((c1 | c2).check(5));
        REQUIRE((c1 | c2).check(11));
        REQUIRE((c1 | c2).check(2));

        REQUIRE((!c1).check(2));
        REQUIRE_FALSE((!c1).check(5));
    }

}



TEST_CASE("query_t") {

    REQUIRE(2 + 2 == 4);

    SECTION("query_t") {

    }

}
