#include <catch2/catch.hpp>
#include <components/btree_storage/serialize.hpp>
#include <components/document/mutable/mutable_array.h>
#include <components/document/mutable/mutable_dict.h>

using namespace components::btree;

document::impl::array_t *gen_array(int num) {
    auto array = document::impl::mutable_array_t::new_array().detach();
    for (int i = 0; i < 5; ++i) {
        array->append(num + i);
    }
    return array;
}

document::impl::dict_t *gen_dict(int num) {
    auto dict = document::impl::mutable_dict_t::new_dict().detach();
    dict->set("odd", num % 2 != 0);
    dict->set("even", num % 2 == 0);
    dict->set("three", num % 3 == 0);
    dict->set("five", num % 5 == 0);
    return reinterpret_cast<document::impl::dict_t *>(dict);
}

input_document_t gen_input_doc(int num) {
    input_document_t doc;
    doc.add_string("_id", std::to_string(num));
    doc.add_long("count", num);
    doc.add_string("countStr", std::to_string(num));
    doc.add_double("countDouble", float(num) + 0.1);
    doc.add_bool("countBool", num % 2 != 0);
    doc.add_array("countArray", gen_array(num));
    doc.add_dict("countDict", gen_dict(num));
    auto array = document::impl::mutable_array_t::new_array().detach();
    for (int i = 0; i < 5; ++i) {
        array->append(gen_array(num + i));
    }
    doc.add_array("nestedArray", array);
    array = document::impl::mutable_array_t::new_array().detach();
    for (int i = 0; i < 5; ++i) {
        auto dict = document::impl::mutable_dict_t::new_dict().detach();
        dict->set("number", num + i);
        array->append(dict);
    }
    doc.add_array("dictArray", array);
    auto dict = document::impl::mutable_dict_t::new_dict().detach();
    for (int i = 0; i < 5; ++i) {
        dict->set(std::to_string(num + i), gen_dict(num + i));
    }
    doc.add_dict("mixedDict", dict);
    return doc;
}

document_unique_ptr gen_doc(int num) {
    return make_document(gen_input_doc(num));
}


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