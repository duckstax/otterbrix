#include <catch2/catch.hpp>

#include "msgpack.hpp"

#include "components/document/document.hpp"
#include "components/document/document_view.hpp"
#include "components/document/msgpack/msgpack_encoder.hpp"
#include "components/tests/generaty.hpp"

using namespace components::document;

TEST_CASE("natyve pack document") {
    auto doc1 = gen_doc(10);
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, doc1);

    msgpack::unpacked msg;
    msgpack::unpack(msg, sbuf.data(), sbuf.size());

    msgpack::object obj = msg.get();
    auto doc2 = obj.as<document_ptr>();

    document_view_t view1(doc1);
    document_view_t view2(doc2);

    REQUIRE(view1.count() == view2.count());
    REQUIRE(view1.get_string("_id") == view2.get_string("_id"));
    REQUIRE(view1.get_long("count") == view2.get_long("count"));
    REQUIRE(view1.get_string("countStr") == view2.get_string("countStr"));
    REQUIRE(view1.get_double("countDouble") == Approx(view2.get_double("countDouble")));
    REQUIRE(view1.get_bool("countBool") == view2.get_bool("countBool"));
    REQUIRE(view1.get_array("countArray").count() == view2.get_array("countArray").count());
    REQUIRE(view1.get_dict("countDict").count() == view2.get_dict("countDict").count());
}



TEST_CASE("natyve pack document  and zone") {
    auto doc1 = gen_doc(10);
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, doc1);

    msgpack::zone zone;
    msgpack::object obj =  msgpack::unpack(zone, sbuf.data(), sbuf.size());
    auto doc2 = obj.as<document_ptr>();

    document_view_t view1(doc1);
    document_view_t view2(doc2);

    REQUIRE(view1.count() == view2.count());
    REQUIRE(view1.get_string("_id") == view2.get_string("_id"));
    REQUIRE(view1.get_long("count") == view2.get_long("count"));
    REQUIRE(view1.get_string("countStr") == view2.get_string("countStr"));
    REQUIRE(view1.get_double("countDouble") == Approx(view2.get_double("countDouble")));
    REQUIRE(view1.get_bool("countBool") == view2.get_bool("countBool"));
    REQUIRE(view1.get_array("countArray").count() == view2.get_array("countArray").count());
    REQUIRE(view1.get_dict("countDict").count() == view2.get_dict("countDict").count());
}