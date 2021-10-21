#include <catch2/catch.hpp>
#include "services/storage/query.hpp"

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

    // operators
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

    REQUIRE((query("name") == "Rex")->check(doc));
    REQUIRE((query("type") != "cat")->check(doc));

    // functions
    REQUIRE(eq("male", true)->check(doc));
    REQUIRE(ne("male", false)->check(doc));

    REQUIRE(eq("age", 6)->check(doc));
    REQUIRE(gt("age", 4)->check(doc));
    REQUIRE(lt("age", 7)->check(doc));
    REQUIRE(gte("age", 4)->check(doc));
    REQUIRE(lte("age", 6)->check(doc));
    REQUIRE_FALSE(ne("age", 6)->check(doc));
    REQUIRE_FALSE(gt("age", 7)->check(doc));
    REQUIRE_FALSE(lt("age", 6)->check(doc));
    REQUIRE_FALSE(gte("age", 7)->check(doc));
    REQUIRE_FALSE(lte("age", 5)->check(doc));

    REQUIRE(eq("name", "Rex")->check(doc));
    REQUIRE(ne("type", "cat")->check(doc));
}


TEST_CASE("query_t between/any/all") {
    auto doc = gen_doc();
    std::vector<int> v1 = {3, 4, 5, 6, 7};
    std::vector<int> v2 = {6, 6, 6};

    REQUIRE(between("age", 4, 6)->check(doc));
    REQUIRE_FALSE(between("age", 1, 5)->check(doc));
    REQUIRE_FALSE(between("age", 7, 12)->check(doc));

    REQUIRE(any("age", v1)->check(doc));
    REQUIRE(any("age", v2)->check(doc));

    REQUIRE_FALSE(all("age", v1)->check(doc));
    REQUIRE(all("age", v2)->check(doc));
}


TEST_CASE("query_t regex") {
    auto doc = gen_doc();

    REQUIRE_FALSE(matches("name", "Re")->check(doc));
    REQUIRE(matches("name", "Re.*")->check(doc));
    REQUIRE_FALSE(matches("type", "og")->check(doc));
    REQUIRE(matches("type", ".*og")->check(doc));
    REQUIRE(matches("type", "dog")->check(doc));
    REQUIRE_FALSE(matches("type", "Dog")->check(doc));
}


TEST_CASE("query_t and/or/not") {
    auto doc = gen_doc();

    REQUIRE((query("name") == "Rex" & query("age") == 6)->check(doc));
    REQUIRE_FALSE((query("name") == "Rex" & query("age") == 5)->check(doc));
    REQUIRE_FALSE((query("name") == "Re" & query("age") == 6)->check(doc));
    REQUIRE_FALSE((query("name") == "Re" & query("age") == 5)->check(doc));

    REQUIRE((query("name") == "Rex" | query("age") == 6)->check(doc));
    REQUIRE((query("name") == "Rex" | query("age") == 5)->check(doc));
    REQUIRE((query("name") == "Re" | query("age") == 6)->check(doc));
    REQUIRE_FALSE((query("name") == "Re" | query("age") == 5)->check(doc));

    REQUIRE_FALSE((!(query("name") == "Rex"))->check(doc));
    REQUIRE((!(query("name") == "Re"))->check(doc));
}
