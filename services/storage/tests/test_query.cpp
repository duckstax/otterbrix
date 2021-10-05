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

    REQUIRE((query_t<bool>("male") == true).check(doc));
    REQUIRE((query_t<bool>("male") != false).check(doc));

    REQUIRE((query_t<long>("age") == 6).check(doc));
    REQUIRE((query_t<long>("age") > 4).check(doc));
    REQUIRE((query_t<long>("age") < 7).check(doc));
    REQUIRE((query_t<long>("age") >= 4).check(doc));
    REQUIRE((query_t<long>("age") <= 6).check(doc));
    REQUIRE_FALSE((query_t<long>("age") != 6).check(doc));
    REQUIRE_FALSE((query_t<long>("age") > 7).check(doc));
    REQUIRE_FALSE((query_t<long>("age") < 6).check(doc));
    REQUIRE_FALSE((query_t<long>("age") >= 7).check(doc));
    REQUIRE_FALSE((query_t<long>("age") <= 5).check(doc));

    REQUIRE((query_t<std::string>("name") == "Rex").check(doc));
    REQUIRE((query_t<std::string>("type") != "cat").check(doc));
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


//TEST_CASE("query_t and/or/not") {
//    auto c1 = query_t<int>([](int v) { return v > 3; });
//    auto c2 = query_t<int>([](int v) { return v < 10; });

//    REQUIRE((c1 & c2).check(5));
//    REQUIRE_FALSE((c1 & c2).check(11));
//    REQUIRE_FALSE((c1 & c2).check(2));

//    REQUIRE((c1 | c2).check(5));
//    REQUIRE((c1 | c2).check(11));
//    REQUIRE((c1 | c2).check(2));

//    REQUIRE((!c1).check(2));
//    REQUIRE_FALSE((!c1).check(5));
//}
