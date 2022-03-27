#include <catch2/catch.hpp>

#include "msgpack.hpp"

#include "components/document/document.hpp"
#include <components/document/mutable/mutable_array.h>
#include <components/document/mutable/mutable_dict.h>
#include "components/document/msgpack/msgpack_encoder.hpp"

using namespace components::document;


document::retained_t<document::impl::array_t> gen_array(int num) {
    auto array = document::impl::mutable_array_t::new_array();
    for (int i = 0; i < 5; ++i) {
        array->append(num + i);
    }
    return array;
}

document::retained_t<document::impl::dict_t> gen_dict(int num) {
    auto dict = document::impl::mutable_dict_t::new_dict();
    dict->set("odd", num % 2 != 0);
    dict->set("even", num % 2 == 0);
    dict->set("three", num % 3 == 0);
    dict->set("five", num % 5 == 0);
    return dict;
}

document_ptr gen_doc(int num) {
    auto doc = document::impl::mutable_dict_t::new_dict();
    doc->set("_id", std::to_string(num));
    doc->set("count", num);
    doc->set("countStr", std::to_string(num));
    doc->set("countDouble", float(num) + 0.1);
    doc->set("countBool", num % 2 != 0);
    doc->set("countArray", gen_array(num));
    doc->set("countDict", gen_dict(num));
    auto array = document::impl::mutable_array_t::new_array();
    for (int i = 0; i < 5; ++i) {
        array->append(gen_array(num + i));
    }
    doc->set("nestedArray", array);
    array = document::impl::mutable_array_t::new_array();
    for (int i = 0; i < 5; ++i) {
        auto dict = document::impl::mutable_dict_t::new_dict();
        dict->set("number", num + i);
        array->append(dict);
    }
    doc->set("dictArray", array);
    auto dict = document::impl::mutable_dict_t::new_dict();
    for (int i = 0; i < 5; ++i) {
        dict->set(std::to_string(num + i), gen_dict(num + i));
    }
    doc->set("mixedDict", dict);
    return make_document(doc);;
}



TEST_CASE("natyve pack document") {
    auto doc1 = gen_doc(10);
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, doc1);

    msgpack::unpacked msg;
    msgpack::unpack(msg, sbuf.data(), sbuf.size());

    msgpack::object obj = msg.get();
    auto doc2 = obj.as<document_ptr>();

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



TEST_CASE("natyve pack document  and zone") {
    auto doc1 = gen_doc(10);
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, doc1);

    msgpack::zone zone;
    msgpack::object obj =  msgpack::unpack(zone, sbuf.data(), sbuf.size());
    auto doc2 = obj.as<document_ptr>();

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