#include <catch2/catch.hpp>
#include "query.hpp"

using namespace services::storage;

document_t gen_doc() {
    document_t doc;
    doc.add("_id", std::string("1"));
    doc.add("name", std::string("Rex"));
    doc.add("type", std::string("dog"));
    doc.add("age", 6l);
    doc.add("male", true);
    return doc;
}

TEST_CASE("query_t create") {
    auto doc = gen_doc();

    REQUIRE((query("male") == true)->check(doc));
    REQUIRE((query("male") != false)->check(doc));

    REQUIRE((query("age") == 6)->check(doc));
    REQUIRE((query("age") > 4)->check(doc));
    REQUIRE((query("age") < 7)->check(doc));
    REQUIRE((query("age") >= 4)->check(doc));
    REQUIRE((query("age") <= 6)->check(doc));
    REQUIRE_FALSE((query("age") != 6)->check(doc));
    REQUIRE_FALSE((query("age") > 7)->check(doc));
    REQUIRE_FALSE((query("age") < 6)->check(doc));
    REQUIRE_FALSE((query("age") >= 7)->check(doc));
    REQUIRE_FALSE((query("age") <= 5)->check(doc));

    REQUIRE((query("name") == std::string("Rex"))->check(doc));
    REQUIRE((query("type") != std::string("cat"))->check(doc));
}


TEST_CASE("query_t between/any/all") {
    auto doc = gen_doc();

    REQUIRE(query_t<long>("age").between(4, 6).check(doc));
    REQUIRE_FALSE(query_t<long>("age").between(1, 5).check(doc));
    REQUIRE_FALSE(query_t<long>("age").between(7, 12).check(doc));

    std::vector<int> v1 = {3, 4, 5, 6, 7};
    std::vector<int> v2 = {6, 6, 6};

    REQUIRE(query_t<int>("age").any(v1).check(doc));
    REQUIRE(query_t<int>("age").any(v2).check(doc));

    REQUIRE_FALSE(query_t<int>("age").all(v1).check(doc));
    REQUIRE(query_t<int>("age").all(v2).check(doc));
}


TEST_CASE("query_t regex") {
    auto doc = gen_doc();

    REQUIRE(query_t<std::string>("name").matches("Re").check(doc));
    REQUIRE(query_t<std::string>("type").matches("og").check(doc));
    REQUIRE_FALSE(query_t<std::string>("type").matches("Dog").check(doc));
}


TEST_CASE("query_t and/or/not") {
    auto doc = gen_doc();

    REQUIRE((query("name") == std::string("Rex") & query("age") == 6)->check(doc));
    REQUIRE_FALSE((query("name") == std::string("Rex") & query("age") == 5)->check(doc));
    REQUIRE_FALSE((query("name") == std::string("Re") & query("age") == 6)->check(doc));
    REQUIRE_FALSE((query("name") == std::string("Re") & query("age") == 5)->check(doc));

    REQUIRE((query("name") == std::string("Rex") | query("age") == 6)->check(doc));
    REQUIRE((query("name") == std::string("Rex") | query("age") == 5)->check(doc));
    REQUIRE((query("name") == std::string("Re") | query("age") == 6)->check(doc));
    REQUIRE_FALSE((query("name") == std::string("Re") | query("age") == 5)->check(doc));

    REQUIRE_FALSE((!(query("name") == std::string("Rex")))->check(doc));
    REQUIRE((!(query("name") == std::string("Re")))->check(doc));
}
