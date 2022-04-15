#include <catch2/catch.hpp>
#include <document/document_id.hpp>

using components::document::document_id_t;

TEST_CASE("document_id") {
    REQUIRE(document_id_t::generate().to_string().size() == 24);
    REQUIRE(document_id_t::generate(2000).get_timestamp() == 2000);
    REQUIRE(document_id_t::generate().get_timestamp() == static_cast<document_id_t::time_value_t>(time(nullptr)));
}
