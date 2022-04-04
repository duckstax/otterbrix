#include <catch2/catch.hpp>

#include <components/document/document_view.hpp>
#include <components/tests/generaty.hpp>

TEST_CASE("serialize document") {
    auto doc1 = gen_doc(10);
    auto buffer = serialize(doc1);
    auto doc2 = deserialize(buffer);
    document_view_t view1(doc1);
    document_view_t view2(doc2);
    REQUIRE(doc1->data.size() == doc2->data.size());
    REQUIRE(view1.count() == view2.count());
    REQUIRE(view1.get_string("_id") == view2.get_string("_id"));
    REQUIRE(view1.get_long("count") == view2.get_long("count"));
    REQUIRE(view1.get_string("countStr") == view2.get_string("countStr"));
    REQUIRE(view1.get_double("countDouble") == Approx(view2.get_double("countDouble")));
    REQUIRE(view1.get_bool("countBool") == view2.get_bool("countBool"));
    REQUIRE(view1.get_array("countArray").count() == view2.get_array("countArray").count());
    REQUIRE(view1.get_dict("countDict").count() == view2.get_dict("countDict").count());
}