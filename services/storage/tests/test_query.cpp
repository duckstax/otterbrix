#include <catch2/catch.hpp>
#include "query.hpp"

using namespace services::storage;

TEST_CASE("query_t create") {
    auto c1 = query_t<bool>([](bool v){ return v == true; });
    REQUIRE(c1.check(true));
    REQUIRE_FALSE(c1.check(false));

    auto c2 = query_t<int>([](int v){ return v == 10; });
    REQUIRE(c2.check(10));
    REQUIRE_FALSE(c2.check(0));

    auto c3 = query_t<std::string>([](std::string v){ return v == "test text"; });
    REQUIRE(c3.check("test text"));
    REQUIRE_FALSE(c3.check("testtext"));
    REQUIRE_FALSE(c3.check("Test text"));
}

TEST_CASE("query_t and/or/not") {
    auto c1 = query_t<int>([](int v) { return v > 3; });
    auto c2 = query_t<int>([](int v) { return v < 10; });

    REQUIRE((c1 & c2).check(5));
    REQUIRE_FALSE((c1 & c2).check(11));
    REQUIRE_FALSE((c1 & c2).check(2));

    REQUIRE((c1 | c2).check(5));
    REQUIRE((c1 | c2).check(11));
    REQUIRE((c1 | c2).check(2));

    REQUIRE((!c1).check(2));
    REQUIRE_FALSE((!c1).check(5));
}

TEST_CASE("query_t between/any/all") {
    REQUIRE(query_t<int>::between(10, 20).check(12));
    REQUIRE_FALSE(query_t<int>::between(10, 20).check(9));
    REQUIRE_FALSE(query_t<int>::between(10, 20).check(21));

    std::vector<int> v1 = {10, 11, 13, 14, 15};
    std::vector<int> v2 = {10, 10, 10};
    REQUIRE(query_t<int>::any(v1).check(11));
    REQUIRE(query_t<int>::any(v1).check(14));
    REQUIRE_FALSE(query_t<int>::any(v1).check(12));
    REQUIRE_FALSE(query_t<int>::any(v1).check(9));

    REQUIRE(query_t<int>::all(v2).check(10));
    REQUIRE_FALSE(query_t<int>::all(v2).check(11));
    REQUIRE_FALSE(query_t<int>::all(v1).check(11));
}

TEST_CASE("query_t regex") {
    REQUIRE(query_t<std::string>::matches("find \\d+").check("This find 100 $"));
    REQUIRE_FALSE(query_t<std::string>::matches("find \\d+").check("This Find 100 $"));
    REQUIRE_FALSE(query_t<std::string>::matches("find \\d+").check("This find n100 $"));
}
