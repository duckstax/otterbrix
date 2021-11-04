#include <catch2/catch.hpp>
#include "document/document.hpp"
#include "document/mutable/mutable_array.h"
#include <iostream>

using namespace components::document;

TEST_CASE("document") {
    document_t doc;
    doc.add_null("null");
    doc.add_bool("bool", true);
    doc.add_ulong("ulong", 1234567890);
    doc.add_long("long", -1234567890);
    doc.add_double("float", 0.12345);
    doc.add_double("double", 0.123456789);
    doc.add_string("string", "text");

    REQUIRE(doc.is_exists("null"));
    REQUIRE(doc.is_exists("string"));
    REQUIRE_FALSE(doc.is_exists("other"));

    REQUIRE(doc.is_null("null"));
    REQUIRE(doc.is_bool("bool"));
    REQUIRE(doc.is_ulong("ulong"));
    REQUIRE(doc.is_long("long"));
    REQUIRE(doc.is_double("double"));
    REQUIRE(doc.is_string("string"));
    REQUIRE_FALSE(doc.is_string("bool"));
    REQUIRE_FALSE(doc.is_string("null"));

    REQUIRE(doc.get_bool("bool") == true);
    REQUIRE(doc.get_ulong("ulong") == 1234567890);
    REQUIRE(doc.get_long("long") == -1234567890);
    REQUIRE(doc.get_double("double") == Approx(0.123456789));
    REQUIRE(doc.get_string("string") == "text");
    REQUIRE_FALSE(doc.get_string("bool") == "text");
    REQUIRE_FALSE(doc.get_string("double") == "0.123456789");

    auto array = ::document::impl::mutable_array_t::new_array();
    array->append(false);
    array->append(100);
    array->append(std::string("text"));
    doc.add_array("array", array);

    REQUIRE(doc.is_array("array"));
    REQUIRE(doc.get_array("array")->count() == 3);

    document_t sub_doc1;
    sub_doc1.add_bool("bool", false);
    sub_doc1.add_long("long", 1);
    sub_doc1.add_double("double", 0.1);
    sub_doc1.add_string("string", "sub_doc_1");
    doc.add_dict("sub_doc1", sub_doc1);

    document_t sub_doc2;
    sub_doc2.add_bool("bool", true);
    sub_doc2.add_long("long", 2);
    sub_doc2.add_double("double", 0.2);
    sub_doc2.add_string("string", "sub_doc_2");
    doc.add_dict("sub_doc2", sub_doc2);

    REQUIRE(doc.is_dict("sub_doc1"));
    REQUIRE(doc.is_dict("sub_doc2"));

    REQUIRE(doc.get_dict("sub_doc1").get_bool("bool") == false);
    REQUIRE(doc.get_dict("sub_doc1").get_long("long") == 1);
    REQUIRE(doc.get_dict("sub_doc1").get_double("double") == Approx(0.1));
    REQUIRE(doc.get_dict("sub_doc1").get_string("string") == "sub_doc_1");

    //std::cout << doc.to_json() << std::endl;
}
