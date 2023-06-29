#include <catch2/catch.hpp>
#include <document/document.hpp>
#include <document/document_view.hpp>
#include <components/tests/generaty.hpp>

using namespace components::document;

TEST_CASE("document_view::json") {
    auto doc = gen_doc(1);
    document_view_t view(doc);
    REQUIRE(view.to_json() == R"({"_id":"000000000000000000000001","count":1,"countStr":"1","countDouble":1.1,"countBool":true,"countArray":[1,2,3,4,5],"countDict":{"odd":true,"even":false,"three":false,"five":false},"nestedArray":[[1,2,3,4,5],[2,3,4,5,6],[3,4,5,6,7],[4,5,6,7,8],[5,6,7,8,9]],"dictArray":[{"number":1},{"number":2},{"number":3},{"number":4},{"number":5}],"mixedDict":{"1":{"odd":true,"even":false,"three":false,"five":false},"2":{"odd":false,"even":true,"three":false,"five":false},"3":{"odd":true,"even":false,"three":true,"five":false},"4":{"odd":false,"even":true,"three":false,"five":false},"5":{"odd":true,"even":false,"three":false,"five":true}}})");
}

TEST_CASE("document::json") {
    auto doc = gen_doc(1);
    auto json = document_to_json(doc);
    REQUIRE(json == R"({"_id":"000000000000000000000001","count":1,"countStr":"1","countDouble":1.1,"countBool":true,"countArray":[1,2,3,4,5],"countDict":{"odd":true,"even":false,"three":false,"five":false},"nestedArray":[[1,2,3,4,5],[2,3,4,5,6],[3,4,5,6,7],[4,5,6,7,8],[5,6,7,8,9]],"dictArray":[{"number":1},{"number":2},{"number":3},{"number":4},{"number":5}],"mixedDict":{"1":{"odd":true,"even":false,"three":false,"five":false},"2":{"odd":false,"even":true,"three":false,"five":false},"3":{"odd":true,"even":false,"three":true,"five":false},"4":{"odd":false,"even":true,"three":false,"five":false},"5":{"odd":true,"even":false,"three":false,"five":true}}})");
    auto doc2 = document_from_json(json);
    REQUIRE(document_to_json(doc) == document_to_json(doc2));
    document_view_t view(doc);
    document_view_t view2(doc2);
    REQUIRE(view.get_string("_id") == view2.get_string("_id"));
    REQUIRE(view.get_ulong("count") == view2.get_ulong("count"));
    REQUIRE(view.get_array("countArray").count() == view2.get_array("countArray").count());
    REQUIRE(view.get_array("countArray").get_as<ulong>(1) == view2.get_array("countArray").get_as<ulong>(1));
    REQUIRE(view.get_dict("countDict").count() == view2.get_dict("countDict").count());
    REQUIRE(view.get_dict("countDict").get_bool("odd") == view2.get_dict("countDict").get_bool("odd"));
}

TEST_CASE("document::serialize") {
    auto doc1 = gen_doc(1);
    auto ser1 = serialize_document(doc1);
    auto doc2 = deserialize_document(ser1);
    auto ser2 = serialize_document(doc2);
    REQUIRE(ser1 == ser2);
}
