#include <catch2/catch.hpp>

#include "generaty.hpp"


TEST_CASE("serialize document") {
    auto doc1 = gen_doc(10);
    auto buffer = serialize(doc1);
    auto doc2 = deserialize(buffer);
    REQUIRE(doc1->data.size() == doc2->data.size());
    REQUIRE(doc1->structure->count() == doc2->structure->count());
    REQUIRE(doc1->structure->get("_id")->as_string() == doc2->structure->get("_id")->as_string());
    REQUIRE(doc1->structure->get("count")->as_int() == doc2->structure->get("count")->as_int());
    REQUIRE(doc1->structure->get("countStr")->as_string() == doc2->structure->get("countStr")->as_string());
    REQUIRE(doc1->structure->get("countDouble")->as_double() == Approx(doc2->structure->get("countDouble")->as_double()));
    REQUIRE(doc1->structure->get("countBool")->as_bool() == doc2->structure->get("countBool")->as_bool());
    REQUIRE(doc1->structure->get("countArray")->as_array()->count() == doc2->structure->get("countArray")->as_array()->count());
    REQUIRE(doc1->structure->get("countDict")->as_dict()->count() == doc2->structure->get("countDict")->as_dict()->count());
}