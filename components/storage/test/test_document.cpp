#include <catch2/catch.hpp>
#include "storage/document.hpp"

using namespace components::storage;

TEST_CASE("document") {

    SECTION("index") {
        std::stringstream data;
        st_document_t doc(&data);
        doc.add_null("null");
        doc.add_bool("bool", true);
        doc.add_long("long", 1234567890);
        doc.add_double("double", 0.123456789);
        doc.add_string("string", "text");

        REQUIRE(doc.is_exists("null"));
        REQUIRE(doc.is_exists("string"));
        REQUIRE_FALSE(doc.is_exists("other"));

        REQUIRE(doc.is_null("null"));
        REQUIRE(doc.is_bool("bool"));
        REQUIRE(doc.is_long("long"));
        REQUIRE(doc.is_double("double"));
        REQUIRE(doc.is_string("string"));
        REQUIRE_FALSE(doc.is_string("bool"));
        REQUIRE_FALSE(doc.is_string("null"));

        REQUIRE(doc.as_bool("bool") == true);
        REQUIRE(doc.as_long("long") == 1234567890);
        REQUIRE(doc.as_double("double") == Approx(0.123456789));
        REQUIRE(doc.as_string("string") == "text");
        REQUIRE_FALSE(doc.as_string("bool") == "text");
        REQUIRE_FALSE(doc.as_string("double") == "0.123456789");
        REQUIRE_FALSE(doc.as_bool("string") == true);

        st_document_t sub_doc1(&data);
        sub_doc1.add_bool("bool", false);
        sub_doc1.add_long("long", 1);
        sub_doc1.add_double("double", 0.1);
        sub_doc1.add_string("string", "sub_doc_1");
        doc.add_dict("sub_doc1", sub_doc1);

        st_document_t sub_doc2(&data);
        sub_doc2.add_bool("bool", true);
        sub_doc2.add_long("long", 2);
        sub_doc2.add_double("double", 0.2);
        sub_doc2.add_string("string", "sub_doc_2");
        doc.add_dict("sub_doc2", sub_doc2);

        REQUIRE(doc.is_dict("sub_doc1"));
        REQUIRE(doc.is_dict("sub_doc2"));

        REQUIRE(doc.as_dict("sub_doc1").as_bool("bool") == false);
        REQUIRE(doc.as_dict("sub_doc1").as_long("long") == 1);
        REQUIRE(doc.as_dict("sub_doc1").as_double("double") == Approx(0.1));
        REQUIRE(doc.as_dict("sub_doc1").as_string("string") == "sub_doc_1");

        std::cout << doc.to_json_index() << std::endl;
    }
}
