#include <catch2/catch.hpp>
#include "value.hpp"
#include "array.hpp"
#include "dict.hpp"
#include "slice_io.hpp"

using namespace document::impl;

TEST_CASE("constants", "[API]") {
    REQUIRE(value_t::null_value);
    REQUIRE(value_t::null_value->type() == value_type::null);

    REQUIRE(array_t::empty_array);
    REQUIRE(array_t::empty_array->type() == value_type::array);
    REQUIRE(array_t::empty_array->count() == 0);

    REQUIRE(dict_t::empty_dict);
    REQUIRE(dict_t::empty_dict->type() == value_type::dict);
    REQUIRE(dict_t::empty_dict->count() == 0);
}

/*
TEST_CASE("encoder_t", "[API]") {

    SECTION("array_t") {
        encoder_t enc;
        enc.begin_array();
        enc.write_int(100);
        enc.write_undefined();
        enc.write_string("string");
        enc.end_array();
        auto doc = enc.finish_doc();
        auto a = doc.root().as_array();

        REQUIRE(a[0].as_int() == 100);
        REQUIRE(a[1].type() == value_type::undefined);
        REQUIRE_FALSE(a[2].as_int());
        REQUIRE(a[2].as_string());
        REQUIRE(a[2].as_string() == "string");

        array_t::iterator_t i(a);
        REQUIRE(i);
        REQUIRE(i.value().as_int() == 100);
        ++i;
        REQUIRE(i);
        REQUIRE(i.value().type() == value_type::undefined);
        ++i;
        REQUIRE(i);
        REQUIRE_FALSE(i.value().as_int());
        REQUIRE(i.value().as_string());
        REQUIRE(i.value().as_string() == "string");
        ++i;
        REQUIRE_FALSE(i);
    }


    SECTION("dict_t") {
        encoder_t enc;
        enc.begin_dict();
        enc["int"] = 100;
        enc["str"] = "string";
        enc["bool"] = true;
        enc.end_dict();
        doc_t doc = enc.finish_doc();

        REQUIRE(doc["int"].type() == value_type::number);
        REQUIRE(doc["int"].as_int() == 100);
        REQUIRE_FALSE(doc["int"].as_string());
        REQUIRE(doc["str"].type() == value_type::string);
        REQUIRE(doc["str"].as_string() == "string");
        REQUIRE_FALSE(doc["str"].as_int());
        REQUIRE(doc["bool"].type() == value_type::boolean);
        REQUIRE(doc["bool"].as_bool() == true);
        REQUIRE_FALSE(doc["bool"].as_string());
    }

}


TEST_CASE("doc_t", "[API]") {

    SECTION("json") {
        dict_t root;
        shared_keys_t sk = shared_keys_t::create();
        auto data = json_coder::from_json(read_file("test/small-test.json"));
        write_to_file(data, "test/small-test.rj");
        doc_t doc(data, trust_type::untrusted, sk);
        REQUIRE(doc.shared_keys() == sk);
        root = doc.root().as_dict();
        REQUIRE(root);
        REQUIRE(root.find_doc() == doc);

        auto id = root.get("id");
        REQUIRE(id);
        REQUIRE(id.find_doc() == doc);
        REQUIRE(root.find_doc() == doc);
    }

    SECTION("internal") {
        dict_t root;
        shared_keys_t sk = shared_keys_t::create();
        doc_t doc(read_file("test/small-test.rj"), trust_type::untrusted, sk);
        REQUIRE(doc.shared_keys() == sk);
        root = doc.root().as_dict();
        REQUIRE(root);
        REQUIRE(root.find_doc() == doc);

        auto id = root.get("id");
        REQUIRE(id);
        REQUIRE(id.find_doc() == doc);
        REQUIRE(root.find_doc());
    }

}


TEST_CASE("key_path_t", "[API]") {

    SECTION("json") {
        auto data = read_file("test/big-test.json");
        write_to_file(json_coder::from_json(data), "test/big-test.rj");
        doc_t doc = doc_t::from_json(data);
        auto root = doc.root();
        REQUIRE(root.as_array().count() == 10);

        document::error_code error = document::error_code::no_error;
        key_path_t p1{"$[3].name", &error};
        auto name = root[p1];
        REQUIRE(name);
        REQUIRE(name.type() == value_type::string);
        REQUIRE(name.as_string() == "Charlie");

        key_path_t p2{"[-1].breed", &error};
        auto breed = root[p2];
        REQUIRE(breed);
        REQUIRE(breed.type() == value_type::string);
        REQUIRE(breed.as_string() == slice_t("Cymric"));
    }

    SECTION("internal") {
        auto data = read_file("test/big-test.rj");
        doc_t doc(data);
        auto root = doc.root();
        REQUIRE(root.as_array().count() == 10);

        document::error_code error = document::error_code::no_error;
        key_path_t p1{"$[3].name", &error};
        auto name = root[p1];
        REQUIRE(name);
        REQUIRE(name.type() == value_type::string);
        REQUIRE(name.as_string() == "Charlie");

        key_path_t p2{"[-1].breed", &error};
        auto breed = root[p2];
        REQUIRE(breed);
        REQUIRE(breed.type() == value_type::string);
        REQUIRE(breed.as_string() == slice_t("Cymric"));
    }

}


TEST_CASE("deep_iterator_t", "[API]") {
    auto data = read_file("test/small-test.rj");
    auto value = value_t::from_data(data);

    deep_iterator_t i1(nullptr);
    REQUIRE_FALSE(i1);
    REQUIRE_FALSE(i1.value());

    auto str = value.as_dict().get("id");
    REQUIRE(str.type() == value_type::string);
    deep_iterator_t i2(str);
    REQUIRE(i2);
    REQUIRE(i2.value() == str);
    REQUIRE(i2.key() == null_slice);
    REQUIRE(i2.index() == 0);
    REQUIRE(i2.depth() == 0);
    i2.next();
    REQUIRE_FALSE(i2);

    for (deep_iterator_t i3(value); i3; ++i3) {
        REQUIRE(i3);
    }
}


TEST_CASE("mutable_array_t", "[API]") {
    mutable_array_t array = mutable_array_t::new_array();
    array.append("dog");
    REQUIRE(array.to_json_string() == "[\"dog\"]");
    REQUIRE(array.count() == 1);
    REQUIRE(array.get(0).as_string() == "dog");

    array.set(0) = 100;
    REQUIRE(array.to_json_string() == "[100]");
    REQUIRE(array.count() == 1);
    REQUIRE(array.get(0).as_int() == 100);
}

TEST_CASE("mutable_dict_t", "[API]") {
    mutable_dict_t dict = mutable_dict_t::new_dict();
    dict.set("dog", "Rex");
    REQUIRE(dict.get("dog"));
    REQUIRE(dict.get("dog").as_string() == "Rex");
    REQUIRE(dict.count() == 1);

    dict.set("age") = 6;
    REQUIRE(dict.count() == 2);
    REQUIRE(dict.to_json_string() == "{\"age\":6,\"dog\":\"Rex\"}");
    REQUIRE(dict.get("age"));
    REQUIRE(dict.get("age").as_int() == 6);
}
*/